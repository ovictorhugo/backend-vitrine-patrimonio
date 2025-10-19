import uuid
from http import HTTPStatus
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_create_notification(client, create_user, create_token):
    source_user = await create_user(username='source', email='source@test.com')
    target_user = await create_user(username='target', email='target@test.com')
    token = create_token(source_user)

    response = client.post(
        '/notifications/',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'target_user_id': str(target_user.id),
            'type': 'SYSTEM_ALERT',
            'detail': {'info': 'Server maintenance scheduled.'},
        },
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()

    assert isinstance(data, dict)

    assert data['type'] == 'SYSTEM_ALERT'
    assert data['detail']['info'] == 'Server maintenance scheduled.'
    assert uuid.UUID(data['id'])

    assert len(data['recipients']) == 1
    assert data['recipients'][0]['target_user']['id'] == str(target_user.id)


@pytest.mark.asyncio
async def test_create_notification_multiple_targets(
    client, create_user, create_token
):
    source_user = await create_user(username='source', email='source@test.com')
    target1 = await create_user(username='target1', email='t1@test.com')
    target2 = await create_user(username='target2', email='t2@test.com')
    token = create_token(source_user)

    response = client.post(
        '/notifications/',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'target_user_id': f'{target1.id};{target2.id}',
            'type': 'MULTI_ALERT',
            'detail': {'msg': 'Hello to multiple users!'},
        },
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()

    assert isinstance(data, dict)

    assert len(data['recipients']) == 2
    recipient_ids = {r['target_user']['id'] for r in data['recipients']}
    assert str(target1.id) in recipient_ids
    assert str(target2.id) in recipient_ids


@pytest.mark.asyncio
async def test_create_notification_all_users(
    client, session, create_user, create_token
):
    source = await create_user(username='source', email='source@test.com')
    user1 = await create_user(username='user1', email='user1@test.com')
    user2 = await create_user(username='user2', email='user2@test.com')
    token = create_token(source)

    response = client.post(
        '/notifications/',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'target_user_id': '*',
            'type': 'BROADCAST',
            'detail': {'text': 'System-wide notice'},
        },
    )

    assert response.status_code == HTTPStatus.CREATED
    data = response.json()

    assert isinstance(data, dict)
    assert len(data['recipients']) == 3


@pytest.mark.asyncio
async def test_create_notification_target_not_found(
    client, create_user, create_token
):
    source_user = await create_user()
    token = create_token(source_user)

    response = client.post(
        '/notifications/',
        headers={'Authorization': f'Bearer {token}'},
        json={
            'target_user_id': str(uuid4()),
            'type': 'SYSTEM_ALERT',
        },
    )
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {
        'detail': 'Target user(s) not found or deactivated'
    }


@pytest.mark.asyncio
async def test_read_geral_notifications(
    client, create_user, create_notification, create_token
):
    user1 = await create_user(username='user1', email='user1@test.com')
    user2 = await create_user(username='user2', email='user2@test.com')
    token_admin = create_token(user1)
    await create_notification(target_user=user1, source_user=user2, type='T1')
    await create_notification(target_user=user1, source_user=user2, type='T2')
    await create_notification(target_user=user2, source_user=user1, type='T3')

    response = client.get(
        '/notifications/', headers={'Authorization': f'Bearer {token_admin}'}
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert len(data['notifications']) == 3


@pytest.mark.asyncio
async def test_read_notifications_for_user(
    client, create_user, create_notification, create_token
):
    user1 = await create_user(username='user1', email='user1@test.com')
    user2 = await create_user(username='user2', email='user2@test.com')
    token_user1 = create_token(user1)

    await create_notification(target_user=user1, source_user=user2, type='T1')
    await create_notification(target_user=user1, source_user=user2, type='T2')
    await create_notification(target_user=user2, source_user=user1, type='T3')

    response = client.get(
        '/notifications/my', headers={'Authorization': f'Bearer {token_user1}'}
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data['notifications']) == 2

    notif = data['notifications'][0]
    assert notif['read_at'] is None
    assert uuid.UUID(notif['id'])
    assert notif['notification']['type'] == 'T2'
    assert notif['notification']['source_user']['id'] == str(user2.id)


@pytest.mark.asyncio
async def test_read_sent_notifications(
    client, create_user, create_notification, create_token
):
    user1 = await create_user(username='sender', email='sender@test.com')
    user2 = await create_user(username='receiver', email='receiver@test.com')
    token_user1 = create_token(user1)

    await create_notification(
        target_user=user2, source_user=user1, type='MSG_FROM_1'
    )
    await create_notification(
        target_user=user1, source_user=user2, type='MSG_FROM_2'
    )

    response = client.get(
        '/notifications/sent',
        headers={'Authorization': f'Bearer {token_user1}'},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()

    assert len(data['notifications']) == 1

    sent_notif = data['notifications'][0]
    assert sent_notif['type'] == 'MSG_FROM_1'
    assert len(sent_notif['recipients']) == 1
    assert sent_notif['recipients'][0]['target_user']['id'] == str(user2.id)


@pytest.mark.asyncio
async def test_read_notifications_empty(client, create_user, create_token):
    user = await create_user()
    token = create_token(user)

    response = client.get(
        '/notifications/my', headers={'Authorization': f'Bearer {token}'}
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['notifications'] == []


@pytest.mark.asyncio
async def test_update_notification_mark_as_read(
    client, create_user, create_notification, create_token
):
    user = await create_user()
    notification = await create_notification(target_user=user)
    token = create_token(user)

    assert notification.read_at is None

    response = client.patch(
        f'/notifications/{notification.id}',
        headers={'Authorization': f'Bearer {token}'},
        json={'read': True},
    )

    assert response.status_code == HTTPStatus.OK
    data = response.json()
    assert data['read_at'] is not None
    assert data['id'] == str(notification.id)


@pytest.mark.asyncio
async def test_update_notification_wrong_user(
    client, create_user, create_notification, create_token
):
    user1 = await create_user(username='user1', email='user1@test.com')
    user2 = await create_user(username='user2', email='user2@test.com')
    notification = await create_notification(target_user=user1)
    token_user2 = create_token(user2)

    response = client.patch(
        f'/notifications/{notification.id}',
        headers={'Authorization': f'Bearer {token_user2}'},
        json={'read': True},
    )

    assert response.status_code == HTTPStatus.FORBIDDEN
    assert response.json() == {'detail': 'Not enough permissions'}


@pytest.mark.asyncio
async def test_delete_notification(
    client, create_user, create_notification, create_token
):
    """Este teste funciona como está, graças à nova fixture."""
    user = await create_user()
    # notification é um objeto UserNotification
    notification = await create_notification(target_user=user)
    token = create_token(user)

    response = client.delete(
        f'/notifications/{notification.id}',  # ID da UserNotification
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.OK
    assert response.json() == {'message': 'Notification deleted'}


@pytest.mark.asyncio
async def test_delete_notification_not_found(
    client, create_user, create_token
):
    user = await create_user()
    token = create_token(user)

    response = client.delete(
        f'/notifications/{uuid4()}',
        headers={'Authorization': f'Bearer {token}'},
    )

    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json() == {'detail': 'Notification not found'}
