from typing import Optional, Tuple
import html as html_lib
from pathlib import Path
import os


def render_item_html(item, index: int, total_items: int) -> str:
    """
    Gera um HTML estilizado para um item do catálogo, inspirado no template do front.
    Cada item ocupa 1 página (page-break-after: always).
    """

    # ---------- CAMPOS BÁSICOS / ESCAPES ----------

    CONSERVATION_MAP = {
      "UNUSED": [
        "Ocioso",
        "Bem permanente em condições de uso, porém sem aproveitamento funcional no setor em que se encontra, carecendo de realocação ou destinação."
      ],
      "RECOVERABLE": [
        "Recuperável",
        "É um bem que não pode ser usado no momento, mas que pode ser consertado com um custo viável."
      ],
      "UNECONOMICAL": [
        "Anti-econômico",
        "É um bem que funciona, mas cujo uso não compensa economicamente porque a manutenção é cara, a eficiência é baixa ou o equipamento ficou obsoleto."
      ],
      "BROKEN": [
        "Quebrado",
        "É um bem que não tem mais condições de uso, porque perdeu suas características essenciais ou porque o reparo custaria mais de 50% do valor de mercado."
      ],
}

    # Nome do material
    material_name = item.asset.material.material_name
    material_name = html_lib.escape(material_name)

    # Descrição do bem
    asset_description = item.asset.asset_description
    asset_description = html_lib.escape(asset_description)

    # Código + dígito verificador
    code_concat = ""
    try:
        asset_code = item.asset.asset_code or ""
        asset_check_digit = item.asset.asset_check_digit or ""
        code_concat = asset_code + "-" + asset_check_digit
    except AttributeError:
        code_concat = getattr(item, "asset_code_with_digit", "") or ""
    asset_code_with_digit = html_lib.escape(code_concat or "Sem código")

    # ATM
    atm_number = None
    try:
        atm_number = item.asset.atm_number
    except AttributeError:
        atm_number = getattr(item, "atm_number", None)
    atm_number_esc = html_lib.escape(atm_number) if atm_number else ""

    # Responsável / curador
    legal_guardian_name = item.asset.legal_guardian.legal_guardians_name
    legal_guardian_name_esc = html_lib.escape(legal_guardian_name) if legal_guardian_name else ""

    # Possui plaqueta?
    is_official = item.asset.is_official
    if is_official is None:
        plaqueta_text = " -"
        bar_color = "#d4d4d8"
    elif is_official:
        plaqueta_text = " Sim"
        bar_color = "#16a34a"
    else:
        plaqueta_text = " Não"
        bar_color = "#f97316"

    # Situação / conservação
    situation = getattr(item, "situation", None) or ""
    conservation_status = getattr(item, "conservation_status", None) or ""

    situation_esc = html_lib.escape(situation) if situation else ""
    conservation_status_esc = html_lib.escape(conservation_status) if conservation_status else ""

    # Justificativa de catálogo
    catalog_description = getattr(item, "catalog_description", None)
    if catalog_description is None:
        catalog_description = getattr(item, "description", None)
    catalog_description_esc = html_lib.escape(catalog_description) if catalog_description else ""

    # Anunciante
    announcer_username = getattr(item, "announcer_username", None)
    announcer_username_esc = html_lib.escape(announcer_username) if announcer_username else ""

    # Parecerista + justificativa do workflow
    workflow_commission_username, workflow_description = get_workflow_info_from_history(item)

    workflow_commission_username_esc = html_lib.escape(
        workflow_commission_username or "Não informado"
    )

    workflow_description_esc = html_lib.escape(
        workflow_description or "Não informado"
    )

    # ATM (Simples, mantido quase igual, apenas garantindo block model)
    if atm_number_esc:
        atm_html = f"""
            <div style="margin-bottom: 5px;">
                <p
                  style="
                    margin: 0;
                    font-weight: 600;
                    font-size: 11px;
                  " > ATM: {atm_number_esc}
                </p>
            </div>
        """
    else:
        atm_html = ""

    # Responsável + plaqueta (Refatorado de Flex para Inline-Block)
    if legal_guardian_name_esc:
        legal_guardian_html = f"""
            <div
              style="
                margin-bottom: 16px;
                font-size: 0; /* Remove espaços brancos entre inline-blocks */
              "
            >
              <div
                style="
                  display: inline-block;
                  vertical-align: middle;
                  background: #e5e7eb;
                  border-radius: 3px;
                  padding: 2px;
                  margin-right: 8px;
                "
              >
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="#6b7280" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" style="display: block;">
                  <path d="M20 21v-2a4 4 0 0 0-3-3.87" />
                  <path d="M7 10a4 4 0 1 1 10 0 4 4 0 1 1-10 0" />
                  <path d="M4 21v-2a4 4 0 0 1 3-3.87" />
                </svg>
              </div>
              
              <div style="display: inline-block; vertical-align: middle; font-size: 12px; color: #000;">
                  <span>{legal_guardian_name_esc}</span>
                  <span style="font-weight: 500; margin-left: 5px;">· Possui plaqueta?</span>
                  <span style="margin-left: 2px;">{plaqueta_text}</span>
              </div>
            </div>
        """
    else:
        legal_guardian_html = ""

    # Justificativa (Refatorado para Table Layout)
    if catalog_description_esc:
        justificativa_html = f"""
      <div
        style="
          display: table;
          width: 100%;
          border-collapse: separate;
          border-spacing: 0;
          margin-bottom: 16px;
        "
      >
        <div
          style="
            display: table-cell;
            width: 8px;
            background-color: #559FB8;
            border: 1px solid #e5e7eb;
            border-right: 0;
            border-radius: 6px 0 0 6px;
            vertical-align: top;
          "
        ></div>

        <div
          style="
            display: table-cell;
            vertical-align: top;
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 0 6px 6px 0;
            padding: 8px 10px 10px 10px;
          "
        >
            <p
              style="
                margin: 0 0 2px 0;
                font-weight: 600;
                font-size: 12px;
              "
            >
              Justificativa
            </p>
            <div
              style="
                font-size: 10px;
                color: #6b7280;
                line-height: 1.4;
              "
            >
             <span style="word-wrap: break-word; overflow-wrap: break-word;">
                {catalog_description_esc}
             </span>
            </div>
        </div>
      </div>
        """
    else:
        justificativa_html = ""

    # Conservação (Refatorado para Table Layout)
    if situation_esc or conservation_status_esc:
        conservacao_titulo = situation_esc or "Estado de conservação"
        conservacao_titulo = CONSERVATION_MAP[conservacao_titulo]
        
        conservacao_html = f"""
      <div
        style="
          display: table;
          width: 100%;
          border-collapse: separate;
          border-spacing: 0;
          margin-bottom: 16px;
        "
      >
        <div
          style="
            display: table-cell;
            width: 8px;
            background-color: #559FB8;
            border: 1px solid #e5e7eb;
            border-right: 0;
            border-radius: 6px 0 0 6px;
            vertical-align: top;
          "
        ></div>

        <div
          style="
            display: table-cell;
            vertical-align: top;
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 0 6px 6px 0;
            padding: 8px 10px 10px 10px;
          "
        >
            <p
              style="
                margin: 0 0 2px 0;
                font-weight: 600;
                font-size: 12px;
              "
            >
              Estado de conservação · {conservacao_titulo[0]}
            </p>
            <div
              style="
                font-size: 10px;
                color: #6b7280;
                line-height: 1.4;
              "
            >
             <span style="word-wrap: break-word; overflow-wrap: break-word;">
                {conservacao_titulo[1]}
             </span>
            </div>
        </div>
      </div>
        """
    else:
        conservacao_html = ""

    # Anunciante (Refatorado para Table Layout + Inline Block no avatar)
    if announcer_username_esc:
        anunciante_html = f"""
      <div
        style="
          display: table; /* MUDANÇA 1: Usar table em vez de flex no container pai */
          width: 100%;
          border-collapse: separate; /* Permite border-radius */
          border-spacing: 0;
          margin-bottom: 16px;
        "
      >
        <div
          style="
            display: table-cell; /* Comporta-se como celula */
            width: 8px;
            background-color: #559FB8;
            border: 1px solid #e5e7eb;
            border-right: 0;
            border-radius: 6px 0 0 6px;
            vertical-align: top;
          "
        ></div>

        <div
          style="
            display: table-cell; /* Comporta-se como celula */
            vertical-align: top;
            background-color: #ffffff;
            border: 1px solid #e5e7eb;
            border-radius: 0 6px 6px 0;
            padding: 8px 10px 10px 10px;
          "
        >
          <div style="margin-bottom: 5px;">
            
            <div
                style="
                  display: inline-block;
                  vertical-align: middle;
                  width: 24px;
                  height: 24px;
                  background: #e5e7eb;
                  border-radius: 3px;
                  text-align: center;
                  padding-top: 3px; /* Ajuste fino para centralizar SVG verticalmente */
                  box-sizing: border-box;
                  margin-right: 8px;
                "
              >
                <svg
                  width="16"
                  height="16"
                  viewBox="0 0 24 24"
                  fill="none"
                  stroke="#6b7280"
                  stroke-width="2"
                  stroke-linecap="round"
                  stroke-linejoin="round"
                >
                  <path d="M20 21v-2a4 4 0 0 0-3-3.87" />
                  <path d="M7 10a4 4 0 1 1 10 0 4 4 0 1 1-10 0" />
                  <path d="M4 21v-2a4 4 0 0 1 3-3.87" />
                </svg>
            </div>

            <div style="display: inline-block; vertical-align: middle;">
                 <span
                  style="
                    display: block;
                    font-size: 10px;
                    color: #6b7280;
                    line-height: 1;
                    margin-bottom: 2px;
                  "
                >
                  Parecerista
                </span>
                <span
                  style="
                    display: block;
                    font-size: 12px;
                    font-weight: 600;
                    color: #111827;
                    line-height: 1.2;
                  "
                >
                  {workflow_commission_username_esc}
                </span>
            </div>
          </div>

          <div style="display: block; width: 100%;">
              <p
                style="
                  margin: 8px 0 2px 0;
                  font-weight: 600;
                  font-size: 12px;
                "
              >
                Justificativa
              </p>
              <div
               style="
                font-size: 10px;
                color: #6b7280;
                line-height: 1.4;
                text-align: justify;
               "
              >
                 <span style="overflow-wrap: break-word; word-wrap: break-word;">
                    {workflow_description_esc}
                 </span>
              </div>
          </div>
        </div>
      </div>
        """
    else:
        anunciante_html = ""

    # Parecerista + justificativa workflow
    if workflow_commission_username_esc or workflow_description_esc:
        parecerista_html = f"""
        <div
            style="
                display: table;
                width: 100%;
                border-collapse: separate;
                border-spacing: 0;
                margin-bottom: 16px;
            "
        >
            <div
                style="
                    display: table-cell;
                    width: 8px;
                    background-color: #559FB8;
                    border: 1px solid #e5e7eb;
                    border-right: 0;
                    border-radius: 6px 0 0 6px;
                    vertical-align: top;
                "
            ></div>

            <div
                style="
                    display: table-cell;
                    vertical-align: top;
                    background-color: #ffffff;
                    border: 1px solid #e5e7eb;
                    border-radius: 0 6px 6px 0;
                    padding: 8px 10px 10px 10px;
                "
            >
                <div style="margin-bottom: 5px;">
                    <div
                        style="
                            display: inline-block;
                            vertical-align: middle;
                            width: 24px;
                            height: 24px;
                            background: #e5e7eb;
                            border-radius: 3px;
                            text-align: center;
                            padding-top: 3px;
                            box-sizing: border-box;
                            margin-right: 8px;
                        "
                    >
                        <svg
                            width="18"
                            height="18"
                            viewBox="0 0 24 24"
                            fill="none"
                            stroke="#6b7280"
                            stroke-width="2"
                            stroke-linecap="round"
                            stroke-linejoin="round"
                        >
                            <path d="M20 21v-2a4 4 0 0 0-3-3.87" />
                            <path d="M7 10a4 4 0 1 1 10 0 4 4 0 1 1-10 0" />
                            <path d="M4 21v-2a4 4 0 0 1 3-3.87" />
                        </svg>
                    </div>

                    <div style="display: inline-block; vertical-align: middle;">
                        <p
                            style="
                                margin: 0;
                                font-size: 12px;
                                color: #6b7280;
                                line-height: 1;
                                margin-bottom: 2px;
                            "
                        >
                            Parecerista
                        </p>
                        <p
                            style="
                                margin: 0;
                                font-size: 12px;
                                font-weight: 500;
                                color: #111827;
                                line-height: 1.2;
                            "
                        >
                            {workflow_commission_username_esc}
                        </p>
                    </div>
                </div>

                <div style="display: block; width: 100%;">
                    <p
                        style="
                            margin: 0 0 2px 0;
                            font-weight: 600;
                            font-size: 12px;
                        "
                    >
                        Justificativa
                    </p>
                    <div
                        style="
                            font-size: 10px;
                            color: #6b7280;
                            line-height: 1.4;
                            width: 100%;
                        "
                    >
                        <span
                            style="
                                display: block;
                                word-wrap: break-word;
                                overflow-wrap: break-word;
                                word-break: break-all;
                            "
                        >
                            {workflow_description_esc}
                        </span>
                    </div>
                </div>
            </div>
        </div>
        """
    else:
      parecerista_html = ""

    # ---------- IMAGENS (Refatorado para Inline-Block ao invés de Grid/Flex) ----------

    IMAGES_DIR = (Path(__file__).resolve().parent.parent / "storage" / "uploads").resolve()
    images = getattr(item, "images", []) or []
    image_cells = []
    
    # Processamento das células de imagem
    for img in images[:4]:
        file_path = getattr(img, "file_path", None)
        if file_path:
            # Seu nome de arquivo fixo ou dinâmico
            #filename = "e28ae247-cfbe-4e3b-9a4c-7450c80a52dd.png" 
            filename = os.path.basename(file_path)
            
            full_path = (IMAGES_DIR / filename).resolve()
            
            if full_path.is_file():
                src = full_path.as_uri()
                src_esc = html_lib.escape(src)
                
                cell = f"""
                <div
                  style="
                    display: inline-block;
                    vertical-align: top;
                    width: 49%;
                    margin-bottom: 5px;
                    margin-right: 1%;
                    box-sizing: border-box;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    background-color: #f3f4f6;
                    height: 180px;
                    
                    /* Essencial para o corte funcionar e não vazar do container */
                    overflow: hidden; 
                    position: relative;
                  "
                >
                  <img
                    src="{src_esc}"
                    style="
                      display: block;          /* Remove espaço extra em baixo da imagem */
                      width: 100%;             /* Força largura total */
                      height: 100%;            /* Força altura total */
                      object-fit: cover;       /* Preenche o container cortando o excesso (zoom) */
                      object-position: center; /* Centraliza a imagem antes de cortar */
                    "
                  />
                </div>
                """
            else:
                # Caminho não existe (Mantido layout centralizado simples)
                cell = """
                <div
                  style="
                    display: inline-block;
                    vertical-align: top;
                    width: 49%;
                    margin-bottom: 5px;
                    margin-right: 1%;
                    box-sizing: border-box;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    background-color: #f3f4f6;
                    height: 180px;
                    text-align: center;
                    line-height: 160px;
                    overflow: hidden;
                  "
                >
                  <span style="font-size: 10px; color: #9ca3af; line-height: normal; display: inline-block; vertical-align: middle;">
                    sem imagem (arquivo não encontrado)
                  </span>
                </div>
                """
        else:
            # Não tem file_path
            cell = """
            <div
              style="
                display: inline-block;
                vertical-align: top;
                width: 49%;
                margin-bottom: 5px;
                margin-right: 1%;
                box-sizing: border-box;
                border: 1px solid #e5e7eb;
                border-radius: 6px;
                background-color: #f3f4f6;
                height: 180px;
                text-align: center;
                line-height: 160px;
                overflow: hidden;
              "
            >
              <span style="font-size: 10px; color: #9ca3af; line-height: normal; display: inline-block; vertical-align: middle;">
                sem imagem
              </span>
            </div>
            """

        image_cells.append(cell)

    # Container das imagens (remove whitespace para inline-blocks funcionarem bem)
    images_html = f"""
        <div style="width: 100%; font-size: 0; text-align: left;">
            {"".join(image_cells)}
        </div>
    """ if image_cells else """
        <div style="padding: 10px; text-align: center; background: #f9fafb; border-radius: 6px;">
          <span style="font-size: 10px; color: #9ca3af;">sem imagens</span>
        </div>
    """


    # ---------- RODAPÉ ----------

    page_number = index + 1
    total_items = total_items or 1

    # ---------- HTML FINAL (ESTRUTURA EM TABELAS) ----------

    ASSETS_DIR = (Path(__file__).resolve().parent.parent / "assets" ).resolve()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    html = f"""
    <div
        style="
            position: relative;       /* Cria o contexto para o rodapé absoluto */
            width: 100%;
            height: 297mm;            /* Altura EXATA da folha A4 */
            box-sizing: border-box;
            background-color: #f9fafb;
            page-break-after: always; /* Garante que o próximo item vá para outra página */
            overflow: hidden;         /* Previne scrollbars indesejadas */
        "
    >
        
        <table
            style="
                width: 100%;
                border-collapse: collapse;
                margin: 0;
                padding: 40px;
            "
        >
            <tr>
                <td style="width: 150px; padding: 32px 48px 12px 48px; vertical-align: middle;">
                    <img src="{SP_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                </td>
               
                <td style="width: 150px; padding: 32px 48px 12px 48px; text-align: right; vertical-align: middle;">
                    <img src="{EE_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                </td>
            </tr>
        </table>

        <div style="padding: 0 52px 36px 52px; padding-bottom: 60px;">
            <section style="display: block; width: 100%; margin-bottom: 20px;">
                <h2
                  style="
                    font-size: 24px;
                    font-weight: 600;
                    margin: 0 0 5px 0;
                  "
                >
                  {material_name}
                </h2>
                <p
                  style="
                    margin: 0 0 15px 0;
                    color: #6b7280;
                    font-size: 10px;
                  "
                >
                  {asset_description}
                </p>

                <div
                    style="
                      display: table;
                      width: 100%;
                      border-collapse: separate;
                      border-spacing: 0;
                      margin-bottom: 10px;
                    "
                  >
                    <div
                      style="
                        display: table-cell;
                        width: 8px;
                        background-color: {bar_color};
                        border: 1px solid #e5e7eb;
                        border-right: 0;
                        border-radius: 6px 0 0 6px;
                        vertical-align: top;
                      "
                    ></div>

                    <div
                      style="
                        display: table-cell;
                        vertical-align: top;
                        background-color: #ffffff;
                        border: 1px solid #e5e7eb;
                        border-radius: 0 6px 6px 0;
                        padding: 10px 10px 0 10px;
                      "
                    >
                        <div style="margin-bottom: 8px;">
                            <div style="display: inline-block; width: 60%; vertical-align: middle;">
                                 <p style="margin: 0; font-weight: 600; font-size: 10px;">
                                    {asset_code_with_digit}
                                  </p>
                            </div>
                            <div style="display: inline-block; width: 39%; text-align: right; vertical-align: middle;">
                                 {atm_html}
                            </div>
                        </div>

                        <div style="font-size: 10px; color: #4b5563;">
                            {legal_guardian_html}
                        </div>
                    </div>
                </div>

                {justificativa_html}
                {conservacao_html}
                {anunciante_html}
                {parecerista_html}
            </section>

            <section style="display: block; width: 100%; margin-top: 10px;">
                {images_html}
            </section>
        </div>

        <div 
            style="
                position: absolute;       /* Absoluto em relação ao wrapper de 297mm */
                bottom: 0;                /* Cola no fundo da folha */
                left: 0;
                right: 0;
                height: 50px;             /* Altura reservada */
                padding: 0 24px 20px 24px;
            "
        >
             <div style="border-top: 1px solid #e5e7eb; padding-top: 10px;">
              <table style="width: 100%; border-collapse: collapse;">
                
                <tr>
                    <td style="text-align: center; padding-bottom: 6px;">
                         <p
                            style="
                              margin: 0;
                              color: #6b7280;
                              font-size: 11px;
                              font-weight: 500;
                            "
                          >
                            Av. Presidente Antônio Carlos, nº 6.627, Belo Horizonte/MG - CEP: 31.270-901
                          </p>
                    </td>
                </tr>

                <tr>
                    <td style="text-align: right; color: #6b7280; font-size: 10px;">
                        Página {page_number} de {total_items}
                    </td>
                </tr>

              </table>
          </div>
        </div>
    </div>
    """
    return html

def get_workflow_info_from_history(item) -> Tuple[Optional[str], Optional[str]]:
    """
    Retorna (workflow_commission_username, workflow_description) a partir de item.workflow_history.

    - commission_username: user.username do primeiro workflow_history com status "REVIEW_REQUESTED_COMISSION"
    - undo_justification: detail.justificativa do primeiro workflow_history com status "DESFAZIMENTO"
    """
    history = getattr(item, "workflow_history", []) or []

    first_commission = next(
        (h for h in history if getattr(h, "workflow_status", None) == "REVIEW_REQUESTED_COMISSION"),
        None,
    )

    first_undo = next(
        (h for h in history if getattr(h, "workflow_status", None) == "DESFAZIMENTO"),
        None,
    )

    # --- Username do parecerista (commission) ---
    commission_username: Optional[str] = None
    if first_commission is not None:
        user = getattr(first_commission, "user", None)
        if user is not None:
            commission_username = getattr(user, "username", None)

    # --- Justificativa (DESFAZIMENTO) ---
    undo_justification: Optional[str] = None
    if first_undo is not None:
        detail = getattr(first_undo, "detail", None)
        if detail is not None:
            # pode ser dict (JSON) ou objeto
            if isinstance(detail, dict):
                undo_justification = detail.get("justificativa")
            else:
                undo_justification = getattr(detail, "justificativa", None)

    return commission_username, undo_justification
