from vitrine import create_app
from dotenv import load_dotenv

if __name__ == "__main__":
    load_dotenv()
    app = create_app()
    app.run(debug=True, host="0.0.0.0", port=8484)
