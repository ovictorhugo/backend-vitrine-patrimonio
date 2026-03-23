from typing import Optional, Tuple
import html as html_lib
from pathlib import Path
from datetime import datetime
import os
from io import BytesIO

from pyhanko.sign import signers, fields
from pyhanko.pdf_utils.incremental_writer import IncrementalPdfFileWriter
from pyhanko.sign.fields import SigFieldSpec 

from pyhanko.pdf_utils.reader import PdfFileReader
from pyhanko.sign.validation import (
    async_validate_pdf_signature, DocumentSecurityStore, 
)
from pyhanko.sign.general import load_cert_from_pemder
from pyhanko_certvalidator import ValidationContext


KEY_PATH = "/https-credentials/vitrinepatrimonio.eng.ufmg.br.key"
CERT_PATH = "/https-credentials/vitrinepatrimonio.eng.ufmg.br.crt"

#KEY_PATH = "local_key.pem"
#CERT_PATH = "local_cert.pem"


async def seal_pdf_digitally(pdf_bytes: bytes) -> bytes:
    """
    Recebe o PDF em bytes e aplica a assinatura digital de forma ASSÍNCRONA.
    """
    
    # 1. Resolver caminhos
    # Ajuste o .parent conforme a profundidade da sua pasta utils.py
    base_folder = (Path(__file__).resolve().parent.parent.parent.parent.parent.parent.parent).resolve()
    full_key_path = base_folder / KEY_PATH
    full_cert_path = base_folder / CERT_PATH
    
    if not full_key_path.exists() or not full_cert_path.exists():
        print(f"⚠️ AVISO: Certificados não encontrados. PDF sem assinatura.")
        return pdf_bytes

    try:
        # 2. Carregar o Assinador
        signer = signers.SimpleSigner.load(
            key_file=str(full_key_path),
            cert_file=str(full_cert_path),
        )

        pdf_buffer_input = BytesIO(pdf_bytes)
        w = IncrementalPdfFileWriter(pdf_buffer_input)

        # 3. Adicionar o campo de assinatura
        field_spec = SigFieldSpec(sig_field_name='SignatureUFMG')
        
        # --- CORREÇÃO AQUI ---
        # Usamos 'fields' diretamente, e não 'signers.fields'
        fields.append_signature_field(w, field_spec)
        # ---------------------

        pdf_buffer_output = BytesIO()

        # 4. Configurar Metadados
        meta = signers.PdfSignatureMetadata(
            field_name='SignatureUFMG',
            reason='Conferido pelo Sistema Vitrine de Patrimônio',
            location='Universidade Federal de Minas Gerais',
            contact_info='https://sistemapatrimonio.eng.ufmg.br'
        )
        
        # 5. Instanciar o PdfSigner
        pdf_signer = signers.PdfSigner(
            signature_meta=meta,
            signer=signer,
        )

        # 6. Assinar de forma assíncrona
        await pdf_signer.async_sign_pdf(
            w, 
            output=pdf_buffer_output
        )

        return pdf_buffer_output.getvalue()

    except Exception as e:
        # Imprime o traceback completo para facilitar o debug se der outro erro
        import traceback
        traceback.print_exc()
        print(f"❌ Erro crítico ao assinar PDF: {e}")
        return pdf_bytes
    

async def verify_pdf_signature(pdf_bytes: bytes) -> dict:
    """
    Verifica se o PDF possui uma assinatura válida feita com o certificado do servidor.
    """
    base_folder = (Path(__file__).resolve().parent.parent.parent / "certs").resolve()
    full_cert_path = base_folder / CERT_PATH

    if not full_cert_path.exists():
        return {
            "valid": False, 
            "message": "Certificado raiz para validação não encontrado no servidor."
        }

    try:
        # 1. Carrega o PDF
        root = PdfFileReader(BytesIO(pdf_bytes))
        
        # 2. Verifica se existem assinaturas
        if not root.embedded_signatures:
            return {
                "valid": False, 
                "message": "O arquivo não possui assinaturas digitais."
            }

        # 3. Configura o Contexto de Validação
        root_cert = load_cert_from_pemder(str(full_cert_path))
        vc = ValidationContext(trust_roots=[root_cert])

        # 4. Carrega o Document Security Store (DSS) corretamente
        # CORREÇÃO AQUI: Instanciamos a classe em vez de acessar atributo
        dss = DocumentSecurityStore(root)

        # 5. Valida a última assinatura
        sig_status = await async_validate_pdf_signature(
            root.embedded_signatures[-1], 
            vc,
            dss
        )

        # 6. Analisa o resultado
        is_intact = sig_status.intact
        is_trusted = sig_status.valid

        if is_intact and is_trusted:
            signer_name = sig_status.signing_cert.subject.human_friendly
            return {
                "valid": True,
                "message": "Assinatura Válida e Íntegra.",
                "signer": signer_name,
                "timestamp": sig_status.validation_time.isoformat() if sig_status.validation_time else None
            }
        elif is_intact and not is_trusted:
            return {
                "valid": False,
                "message": "Documento íntegro, mas certificado não confiável (assinatura desconhecida ou auto-assinada)."
            }
        else:
             return {
                "valid": False,
                "message": "Assinatura INVÁLIDA ou documento alterado."
            }

    except Exception as e:
        print(f"Erro na validação: {e}")
        import traceback
        traceback.print_exc()
        return {"valid": False, "message": "Erro ao processar arquivo PDF."}


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
            position: relative;       
            width: 100%;
            height: 100%;            
            box-sizing: border-box;
            background-color: #f9fafb;
            page-break-after: always; 
            overflow: hidden;         
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


def render_transfer_item(item,signers,location) -> str:
    """
    Gera um HTML estilizado para um item do catálogo, inspirado no template do front.
    Cada item ocupa 1 página (page-break-after: always).
    """

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

   # Anunciante
    announcer_username = getattr(item, "announcer_username", None)
    announcer_username_esc = html_lib.escape(announcer_username) if announcer_username else ""

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

    if legal_guardian_name_esc:
        legal_guardian_html = f"""
            <div
              style="
                margin-top: 4px;
                margin-left: 8px;
                font-size: 0; 
              "
            >            
              <div style="display: inline-block; vertical-align: middle; font-size: 12px; color: #000;">
                  <span>{legal_guardian_name_esc}</span>
              </div>
            </div>
        """
    else:
        legal_guardian_html = ""

    # ---------- IMAGENS (Refatorado para Inline-Block ao invés de Grid/Flex) ----------

    IMAGES_DIR = (Path(__file__).resolve().parent.parent / "storage" / "uploads").resolve()
    images = getattr(item, "images", []) or []
    image_cells = []

    # Processamento das imagens (Limita a 4 para manter o grid 2x2)
    for img in images[:4]:
        file_path = getattr(img, "file_path", None)
        has_image = False
        src_esc = ""

        # Verifica arquivo
        if file_path:
            filename = os.path.basename(file_path)
            full_path = (IMAGES_DIR / filename).resolve()
            if full_path.is_file():
                has_image = True
                src = full_path.as_uri()
                src_esc = html_lib.escape(src)

        # Estilo base da CÉLULA (Item do Grid)
        # Nota: Não definimos width nem margin aqui. O Grid Pai controla isso.
        cell_style = """
            background-color: #f3f4f6;
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            height: 140px;
            overflow: hidden; 
            position: relative;
            box-sizing: border-box;
        """

        if has_image:
            cell = f"""
            <div style="{cell_style}">
              <img
                src="{src_esc}"
                style="
                  display: block;
                  width: 100%;
                  height: 100%;
                  object-fit: cover;
                  object-position: center;
                "
              />
            </div>
            """
        else:
            # Placeholder para imagem ausente
            # Usamos Flexbox DENTRO da célula apenas para centralizar o texto de erro
            label = "sem imagem (arquivo não encontrado)" if file_path else "sem imagem"
            cell = f"""
            <div style="{cell_style} display: flex; align-items: center; justify-content: center; text-align: center;">
              <span style="font-size: 10px; color: #9ca3af; padding: 0 10px;">
                {label}
              </span>
            </div>
            """

        image_cells.append(cell)

    # Container das imagens usando CSS GRID
    # grid-template-columns: 1fr 1fr -> Cria 2 colunas de larguras iguais
    images_html = f"""
        <div style="
            display: grid;
            grid-template-columns: 1fr 1fr; 
            gap: 8px;
            width: 100%;
            margin-bottom:8px;
        ">
            {"".join(image_cells)}
        </div>
    """ if image_cells else """
        <div style="padding: 10px; text-align: center; background: #f9fafb; border-radius: 6px;">
          <span style="font-size: 10px; color: #9ca3af;">sem imagens</span>
        </div>
    """


    # ------------- Assinaturas -----------------------

    signature_cells = []

    for signer in signers:
        
      if(signer.user_id):
        role = signer.user.username
        name = signer.user.email
      else:
        role = "Chefe de departamento não identificado"
        name = ""

      if(signer.signedAt):
        date_val = signer.signedAt
      else:
        date_val = 0 
        
      # Lógica de Estado: Assinado vs Pendente
      if date_val:
          # ASSINADO: Cor Verde (ou a Azul #559FB8 que você usava), Data preenchida
          bar_color = "#10B981" # Verde esmeralda para sucesso
          # bar_color = "#559FB8" # Azul original (opcional)
          date_html = f'<span style="color: #111827;">{date_val}</span>'
          status_label = "Data:"
      else:
          # NÃO ASSINADO: Cor Vermelha, Data em branco
          bar_color = "#EF4444" # Vermelho
          date_html = '<span style="color: #9ca3af;">(aguardando assinatura)</span>'
          status_label = "Data:"

      cell = f"""
      <div
          style="
            display: table;
            width: 100%;
            border-collapse: separate;
            border-spacing: 0;
            /* Removemos margin-bottom aqui pois o gap do grid cuida disso */
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
              padding: 10px;
            "
          >
              <p
                style="
                  margin: 0 0 4px 0;
                  font-weight: 700;
                  font-size: 11px;
                  color: #374151;
                  text-transform: uppercase;
                "
              >
                {role}
              </p>
              
              <div style="font-size: 12px; color: #111827; margin-bottom: 4px; font-weight: 500;">
                  {name}
              </div>

              <div
                style="
                  font-size: 10px;
                  color: #6b7280;
                  line-height: 1.4;
                  border-top: 1px dashed #e5e7eb;
                  padding-top: 4px;
                "
              >
                <strong>{status_label}</strong> {date_html}
              </div>
          </div>
      </div>
      """
      signature_cells.append(cell)

    # 2. Container GRID 2x2
    signatures_html = f"""
        <div style="
            display: grid;
            grid-template-columns: 1fr 1fr; 
            gap: 12px;
            width: 100%;
            margin-top: 20px;
            page-break-inside: avoid;
        ">
            {"".join(signature_cells)}
        </div>
    """


    # ---------- HTML FINAL (ESTRUTURA EM TABELAS) ----------

    ASSETS_DIR = (Path(__file__).resolve().parent.parent / "assets" ).resolve()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    html = f"""
    <div
        style="
            position: relative;
            width: 100%;
            height: 297mm;
            box-sizing: border-box;
            background-color: #f9fafb;
            overflow: hidden;
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
                    font-size: 20px;
                    font-weight: 700;
                    margin: 0 0 15px 0;
                    text-transform: uppercase;
                    color: #111827;
                    text-align: center;
                  "
                >
                  Transferência Interna de Material Permanente<br>Escola de Engenharia
                </h2>
                
                <div style="
                    background-color: #fffbeb; 
                    border: 1px solid #fcd34d; 
                    color: #92400e; 
                    padding: 10px; 
                    border-radius: 6px; 
                    font-size: 10px; 
                    margin-bottom: 15px;
                ">
                   <strong style="color: #78350f;">ORIENTAÇÕES IMPORTANTES:</strong><br>
                    1. As assinaturas devem ser realizadas exclusivamente através do <strong>Sistema Patrimônio</strong>.<br>
                    2. Todas as partes envolvidas já foram devidamente notificadas por e-mail.<br>
                    3. Assim que todas as assinaturas forem registradas, um novo e-mail será enviado a todas as partes contendo a versão definitiva deste documento.
                </div>

                <div style="
                    background-color: #ffffff;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    margin-bottom: 15px;
                    overflow: hidden;
                ">
                    <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
                        <thead>
                            <tr style="background-color: #f3f4f6; border-bottom: 1px solid #e5e7eb;">
                                <th style="padding: 8px 12px; text-align: left; width: 15%; color: #374151; font-weight: 600;">CÓDIGO</th>
                                <th style="padding: 8px 12px; text-align: left; width: 30%; color: #374151; font-weight: 600;">NOME DO BEM</th>
                                <th style="padding: 8px 12px; text-align: left; width: 55%; color: #374151; font-weight: 600;">DESCRIÇÃO</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr>
                                <td style="padding: 10px 12px; vertical-align: top; color: #111827;">
                                    {asset_code_with_digit}
                                </td>
                                <td style="padding: 10px 12px; vertical-align: top; color: #111827;">
                                    {material_name}
                                </td>
                                <td style="padding: 10px 12px; vertical-align: top; color: #6b7280;">
                                    {asset_description}
                                </td>
                            </tr>
                        </tbody>
                    </table>
                </div>


                <section style="display: block; width: 100%; margin-top: 10px;">
                {images_html}
                </section>

                <table style="width: 100%; border-collapse: separate; border-spacing: 0; margin-bottom: 15px;">
                    <tr>
                        <td style="width: 50%; vertical-align: top; padding-right: 6px;">
                            <div style="
                                background-color: #ffffff;
                                border: 1px solid #e5e7eb;
                                border-radius: 6px;
                                padding: 12px;
                            ">
                                <h3 style="margin: 0 0 10px 0; font-size: 11px; font-weight: 700; color: #111827; border-bottom: 1px solid #e5e7eb; padding-bottom: 5px;">
                                    LOCAL DE ORIGEM DO BEM
                                </h3>
                                
                                <table style="width: 100%; font-size: 10px; color: #374151; border-collapse: collapse;">
                                    <tr><td style="padding: 4px 0;"><strong>Guardião: </strong> {signers[0].user.username}</td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Unidade: {item.location.sector.agency.agency_name}</strong></td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Depto./Setor: </strong> {item.location.sector.sector_name}</td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Sala: </strong>{item.location.location_name}</td></tr>
                                    </table>
                            </div>
                        </td>

                        <td style="width: 50%; vertical-align: top; padding-left: 6px;">
                            <div style="
                                background-color: #ffffff;
                                border: 1px solid #e5e7eb;
                                border-radius: 6px;
                                padding: 12px;
                            ">
                                <h3 style="margin: 0 0 10px 0; font-size: 11px; font-weight: 700; color: #111827; border-bottom: 1px solid #e5e7eb; padding-bottom: 5px;">
                                    LOCAL DE DESTINO DO BEM
                                </h3>

                                <table style="width: 100%; font-size: 10px; color: #374151; border-collapse: collapse;">
                                    <tr><td style="padding: 4px 0;"><strong>Guardião: </strong> {signers[1].user.username}</td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Unidade: {location.sector.agency.agency_name}</strong></td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Depto./Setor: </strong> {location.sector.sector_name}</td></tr>
                                    <tr><td style="padding: 4px 0;"><strong>Sala: </strong>{location.location_name}</td></tr>
                                    </table>
                            </div>
                        </td>
                    </tr>
                </table>
            </section>
                {signatures_html}
        </div>

        


        <div 
            style="
                position: absolute;
                bottom: 0;
                left: 0;
                right: 0;
                height: 50px;
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
                        Página 1 de 1
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


def render_loanable_item(item) -> str:
    """
    Gera um HTML estilizado para um item de empréstumo.
    """

    # Nome do material
    material_name = item.catalog.asset.material.material_name
    material_name = html_lib.escape(material_name)

    # Descrição do bem
    asset_description = item.catalog.asset.asset_description
    asset_description = html_lib.escape(asset_description)

    # Código + dígito verificador
    code_concat = ""
    try:
        asset_code = item.catalog.asset.asset_code or ""
        asset_check_digit = item.catalog.asset.asset_check_digit or ""
        code_concat = asset_code + "-" + asset_check_digit
    except AttributeError:
        code_concat = getattr(item, "asset_code", "") or ""

    # ATM
    atm_number = None
    try:
        atm_number = item.catalog.asset.atm_number
    except AttributeError:
        atm_number = getattr(item, "atm_number", None)
    atm_number_esc = html_lib.escape(atm_number) if atm_number else ""

    # Responsável / curador
    legal_guardian_name = item.catalog.asset.legal_guardian.legal_guardians_name
    legal_guardian_name_esc = html_lib.escape(legal_guardian_name) if legal_guardian_name else ""


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

    if legal_guardian_name_esc:
        legal_guardian_html = f"""
            <div
              style="
                margin-top: 4px;
                margin-left: 8px;
              "
            >            
              <div style="display: inline-block; vertical-align: middle; font-size: 12px; font-weight: 600px; color: #000;">
                  <span>{legal_guardian_name_esc}</span>
              </div>
            </div>
        """
    else:
        legal_guardian_html = ""

    # ---------- IMAGENS (Refatorado para Inline-Block ao invés de Grid/Flex) ----------

    IMAGES_DIR = (Path(__file__).resolve().parent.parent / "storage" / "uploads").resolve()
    images = getattr(item, "images", []) or []
    image_cells = []

    # Processamento das imagens (Limita a 4 para manter o grid 2x2)
    for img in images[:4]:
        file_path = getattr(img, "file_path", None)
        has_image = False
        src_esc = ""

        # Verifica arquivo
        if file_path:
            filename = os.path.basename(file_path)
            full_path = (IMAGES_DIR / filename).resolve()
            if full_path.is_file():
                has_image = True
                src = full_path.as_uri()
                src_esc = html_lib.escape(src)

        # Estilo base da CÉLULA (Item do Grid)
        # Nota: Não definimos width nem margin aqui. O Grid Pai controla isso.
        cell_style = """
            background-color: #f3f4f6;
            border: 1px solid #e5e7eb;
            border-radius: 6px;
            height: 140px;
            overflow: hidden; 
            position: relative;
            box-sizing: border-box;
        """

        if has_image:
            cell = f"""
            <div style="{cell_style}">
              <img
                src="{src_esc}"
                style="
                  display: block;
                  width: 100%;
                  height: 100%;
                  object-fit: cover;
                  object-position: center;
                "
              />
            </div>
            """
        else:
            # Placeholder para imagem ausente
            # Usamos Flexbox DENTRO da célula apenas para centralizar o texto de erro
            label = "sem imagem (arquivo não encontrado)" if file_path else "sem imagem"
            cell = f"""
            <div style="{cell_style} display: flex; align-items: center; justify-content: center; text-align: center;">
              <span style="font-size: 10px; color: #9ca3af; padding: 0 10px;">
                {label}
              </span>
            </div>
            """

        image_cells.append(cell)

    # Container das imagens usando CSS GRID
    # grid-template-columns: 1fr 1fr -> Cria 2 colunas de larguras iguais
    images_html = f"""
        <div style="
            display: grid;
            grid-template-columns: 1fr 1fr; 
            gap: 8px;
            width: 100%;
            margin-bottom:8px;
        ">
            {"".join(image_cells)}
        </div>
    """ if image_cells else """
        <div style="padding: 10px; text-align: center; background: #f9fafb; border-radius: 6px;">
          <span style="font-size: 10px; color: #9ca3af;">sem imagens</span>
        </div>
    """

    def format_pdf_date(dt) -> str:
        if not dt: return "-"
        # Caso dt seja datetime, apenas formata
        if isinstance(dt, datetime):
            return dt.strftime("%d/%m/%Y %H:%M")
        return str(dt)

    # Vamos gerar as linhas da tabela iterando sobre "loan.loans"
    loans_list = getattr(item, 'loans', [])
    loans_html_rows = []

    if not loans_list:
        loans_html_rows.append("""
            <tr style="border-bottom: 1px solid #e5e7eb;">
                <td colspan="5" style="padding: 15px 12px; text-align: center; color: #6b7280;">
                    Nenhum histórico de empréstimo registrado para este item.
                </td>
            </tr>
        """)
    else:
        for l in loans_list:
            # 1. Nomes dos usuários
            requester_name = getattr(l.requester, 'username', getattr(l.requester, 'email', 'N/A')) if l.requester else 'N/A'
            guardian_name = getattr(l.temporary_guardian, 'username', getattr(l.temporary_guardian, 'email', 'N/A')) if l.temporary_guardian else 'N/A'

            # 2. Definição do Status e Cores da Badge
            status_text = "EMPRESTADO"
            color_bg = "#dbeafe" # Azul claro
            color_text = "#1d4ed8" # Azul escuro

            # Verifica se está atrasado (ainda não devolvido)
            is_atrasado = False
            if l.end_at and not l.is_returned:
                try:
                    is_atrasado = l.end_at.replace(tzinfo=None) < datetime.now()
                except:
                    pass

            # Verifica se FOI devolvido em atraso (ignorando as horas, comparando apenas as datas)
            is_devolvido_em_atraso = False
            if l.is_returned and l.end_at and l.returned_at:
                try:
                    end_date = l.end_at.replace(tzinfo=None).date()
                    returned_date = l.returned_at.replace(tzinfo=None).date()
                    if returned_date > end_date:
                        is_devolvido_em_atraso = True
                except:
                    pass

            # Nova condição de Recusa (Prioridade alta)
            if l.rejection_reason:
                status_text = "RECUSADO"
                color_bg = "#e0e7ff" # Azul claro (indigo-100)
                color_text = "#1e3a8a" # Azul escuro (indigo-900)
            elif l.is_maintenance:
                status_text = "MANUTENÇÃO"
                color_bg = "#fef3c7" # Amarelo claro
                color_text = "#b45309" # Amarelo escuro
            elif l.is_returned and not l.is_confirmed:
                # Mantido como fallback caso seja recusado sem motivo escrito
                status_text = "RECUSADO"
                color_bg = "#fee2e2" # Vermelho claro
                color_text = "#b91c1c" # Vermelho escuro
            elif is_devolvido_em_atraso:
                status_text = "DEVOLVIDO EM ATRASO"
                color_bg = "#fee2e2" # Vermelho claro
                color_text = "#b91c1c" # Vermelho escuro
            elif l.is_returned:
                status_text = "DEVOLVIDO"
                color_bg = "#d1fae5" # Verde claro
                color_text = "#047857" # Verde escuro
            elif not l.is_executed:
                status_text = "PEDIDO"
                color_bg = "#f3f4f6" # Cinza claro
                color_text = "#374151" # Cinza escuro
            elif is_atrasado:
                status_text = "ATRASADO"
                color_bg = "#fee2e2"
                color_text = "#b91c1c"

            # 3. Estrutura a linha principal
            border_style = "" if (l.lend_detail or l.rejection_reason) else "border-bottom: 1px solid #e5e7eb;"
            
            row_html = f"""
            <tr style="{border_style}">
                <td style="padding: 10px 12px; vertical-align: top;">
                    <span style="display: inline-block; text-align: center; background-color: {color_bg}; color: {color_text}; padding: 3px 6px; border-radius: 4px; font-weight: 700; font-size: 9px;">
                        {status_text}
                    </span>
                </td>
                <td style="padding: 10px 12px; vertical-align: top; color: #111827;">
                    {html_lib.escape(requester_name)}
                </td>
                <td style="padding: 10px 12px; vertical-align: top; color: #111827;">
                    {html_lib.escape(guardian_name)}
                </td>
                <td style="padding: 10px 12px; vertical-align: top; color: #6b7280;">
                    {format_pdf_date(l.start_at)}
                </td>
                <td style="padding: 10px 12px; vertical-align: top; color: #6b7280;">
                    {format_pdf_date(l.returned_at) if l.returned_at else format_pdf_date(l.end_at)}
                </td>
            </tr>
            """
            
            # 4. Linha secundária para observações e motivos de recusa (se existirem)
            if l.lend_detail or l.rejection_reason:
                obs = html_lib.escape(l.lend_detail) if l.lend_detail else ""
                
                # A cor do motivo da recusa foi alterada para azul escuro para combinar com a nova regra
                rej = f"<strong style='color: #1e3a8a;'>Motivo Recusa:</strong> {html_lib.escape(l.rejection_reason)}" if l.rejection_reason else ""
                
                row_html += f"""
                <tr style="border-bottom: 1px solid #e5e7eb; background-color: #f9fafb;">
                    <td colspan="5" style="padding: 4px 12px 10px 12px; font-size: 9px; color: #6b7280; font-style: italic;">
                        <strong>Obs:</strong> {obs} {rej}
                    </td>
                </tr>
                """

            loans_html_rows.append(row_html)

            
    # Une todas as linhas criadas para injetar no HTML
    loans_tbody_html = "".join(loans_html_rows)

    # ---------- HTML FINAL (ESTRUTURA EM TABELAS) ----------

    ASSETS_DIR = (Path(__file__).resolve().parent.parent / "assets" ).resolve()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    html = f"""
    <div
        style="
            position: relative;
            width: 100%;
            height: 297mm;
            box-sizing: border-box;
            background-color: #f9fafb;
            overflow: hidden;
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

        <div style="padding: 0 26px 36px 26px; padding-bottom: 30px;">
            <section style="display: block; width: 100%; margin-bottom: 20px;">
                
                <h2
                  style="
                    font-size: 20px;
                    font-weight: 700;
                    margin: 0 0 15px 0;
                    text-transform: uppercase;
                    color: #111827;
                    text-align: center;
                  "
                >
                  Histórico de Empréstimos
                </h2>

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
                        padding: 10px;
                      "
                    >
                        <div style="margin-bottom: 8px;">
                            <div style="display: inline-block; width: 60%; vertical-align: middle;">
                                 <div style="
                                    display: flex;">
                                    <p style="margin: 0; font-weight: 600; font-size: 16px;">
                                        {material_name}
                                    </p>
                                    <p style="margin: 2px 0 0 8px; font-weight: 600; font-size: 12px;">
                                        {code_concat}
                                    </p>
                                  </div>
                                <div style="
                                display: flex;
                                margin-top:5px;
                                margin-bottom:5px;">
                                <div
                                    style="
                                    display: inline-block;
                                    vertical-align: middle;
                                    width: 20px;
                                    height: 20px;
                                    background: #e5e7eb;
                                    border-radius: 3px;
                                    text-align: center;
                                    box-sizing: border-box;
                                    "
                                >
                                    <svg
                                    width="20"
                                    height="20"
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
                                {legal_guardian_html}
                            </div>
                            </div>
                        </div>

                        <div style="font-size: 10px; color: #4b5563;margin-left:16px;">
                            {asset_description}
                        </div>
                    </div>
                </div>

                <section style="display: block; width: 100%; margin-top: 10px;">
                {images_html}
                </section>
            </section>

            <section style="display: block; width: 100%; margin-top: 20px; margin-bottom: 20px;">
    
                <h3 style="margin: 0 0 10px 0; font-size: 13px; font-weight: 700; color: #111827; border-bottom: 2px solid #e5e7eb; padding-bottom: 5px;">
                    REGISTRO DE EMPRÉSTIMOS
                </h3>

                <div style="
                    background-color: #ffffff;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    overflow: hidden;
                ">
                    <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
                        <thead>
                            <tr style="background-color: #f3f4f6; border-bottom: 1px solid #e5e7eb;">
                                <th style="padding: 8px 12px; text-align: left; width: 14%; color: #374151; font-weight: 600;">STATUS</th>
                                <th style="padding: 8px 12px; text-align: left; width: 25%; color: #374151; font-weight: 600;">SOLICITANTE</th>
                                <th style="padding: 8px 12px; text-align: left; width: 25%; color: #374151; font-weight: 600;">RESPONSÁVEL</th>
                                <th style="padding: 8px 12px; text-align: left; width: 18%; color: #374151; font-weight: 600;">INÍCIO</th>
                                <th style="padding: 8px 12px; text-align: left; width: 18%; color: #374151; font-weight: 600;">FIM / RETORNO</th>
                            </tr>
                        </thead>
                        <tbody>
                            {loans_tbody_html}
                        </tbody>
                    </table>
                </div>
            </section>
        </div>

        <div 
            style="
                position: absolute;
                bottom: 0;
                left: 0;
                right: 0;
                height: 50px;
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
                        Página 1 de 1
                    </td>
                </tr>
              </table>
          </div>
        </div>
    </div>
"""
    return html


def render_all_loanable_items(items: list) -> str:
    """
    Gera um HTML estilizado listando todos os itens de empréstimo disponíveis.
    """
    
    # 1. Gerar as linhas da tabela iterando sobre a lista de itens
    rows_html = []
    
    for item in items:
        # Nome do material
        try:
            material_name = item.catalog.asset.material.material_name
        except AttributeError:
            material_name = "Sem nome"
        material_name_esc = html_lib.escape(material_name)

        # Código + dígito verificador
        try:
            asset_code = item.catalog.asset.asset_code or ""
            asset_check_digit = item.catalog.asset.asset_check_digit or ""
            code_concat = f"{asset_code}-{asset_check_digit}" if asset_check_digit else asset_code
        except AttributeError:
            code_concat = getattr(item, "asset_code", "") or ""
        code_concat_esc = html_lib.escape(code_concat)

        # ATM
        try:
            atm_number = item.catalog.asset.atm_number
        except AttributeError:
            atm_number = getattr(item, "atm_number", None)
        
        # Oculta se for None ou a string "None"
        if not atm_number or str(atm_number).lower() == "none":
            atm_number_esc = "-"
        else:
            atm_number_esc = html_lib.escape(str(atm_number))

        # Cadastrado em (usando created_at do AuditMixin)
        created_at = getattr(item, "created_at", None)
        if created_at:
            if isinstance(created_at, str):
                try:
                    created_date = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    created_str = created_date.strftime("%d/%m/%Y")
                except:
                    created_str = created_at[:10]
            else:
                created_str = created_at.strftime("%d/%m/%Y")
        else:
            created_str = "-"

        # Quantidade de Empréstimos (tamanho do array loans)
        loans = getattr(item, "loans", [])
        loans_count = len(loans)
        
        # --- LÓGICA: Cálculo de dias (Manutenção, Atraso e Empréstimos Normais) ---
        total_delay_days = 0
        total_maintenance_days = 0
        total_loan_days = 0
        now = datetime.now()
        
        for l in loans:
            start_date = getattr(l, "start_at", None)
            is_executed = getattr(l, "is_executed", False)
            is_maintenance = getattr(l, "is_maintenance", False)
            
            # Se não tem data de início ou ainda é só um pedido pendente, ignora o cálculo
            if not start_date or (not is_executed and not is_maintenance):
                continue

            try:
                start_dt = start_date.replace(tzinfo=None).date()
                
                # 1. LÓGICA DE MANUTENÇÃO
                if is_maintenance:
                    end_date_maint = getattr(l, "returned_at", getattr(l, "end_at", None))
                    end_dt = end_date_maint.replace(tzinfo=None).date() if end_date_maint else now.date()
                    
                    # Adicionamos + 1 para que o mesmo dia conte como 1 diária de manutenção
                    maint_days = (end_dt - start_dt).days + 1
                    if maint_days > 0:
                        total_maintenance_days += maint_days
                        
                # 2. LÓGICA DE EMPRÉSTIMO NORMAL E ATRASO
                else:
                    end_date = getattr(l, "end_at", None)
                    returned_date = getattr(l, "returned_at", None)
                    
                    actual_return_dt = returned_date.replace(tzinfo=None).date() if returned_date else now.date()
                    expected_end_dt = end_date.replace(tzinfo=None).date() if end_date else actual_return_dt
                    
                    # A) Cálculo de Atraso
                    # Aqui não soma 1. Se devolveu no dia previsto, atraso é 0. Se devolveu 1 dia depois, atraso é 1.
                    if actual_return_dt > expected_end_dt:
                        delay = (actual_return_dt - expected_end_dt).days
                        total_delay_days += delay
                        
                    # B) Cálculo de Dias Normais
                    normal_end_dt = min(actual_return_dt, expected_end_dt)
                    
                    # Adicionamos + 1 para garantir o mínimo de 1 dia de uso
                    normal_days = (normal_end_dt - start_dt).days + 1
                    
                    if normal_days > 0:
                        total_loan_days += normal_days
                        
            except Exception:
                pass # Ignora erros de data mal formatada no banco
        
        # Formatação visual para a tabela
        delay_str = str(total_delay_days) if total_delay_days > 0 else "-"
        color_delay = "#b91c1c" if total_delay_days > 0 else "#6b7280" # Vermelho
        
        maint_str = str(total_maintenance_days) if total_maintenance_days > 0 else "-"
        color_maint = "#b45309" if total_maintenance_days > 0 else "#6b7280" # Amarelo escuro
        
        loan_days_str = str(total_loan_days) if total_loan_days > 0 else "-"
        color_loan_days = "#059669" if total_loan_days > 0 else "#6b7280" # Verde escuro

        # Cria a linha HTML para este item (Agora com OITO colunas no total)
        row = f"""
        <tr style="border-bottom: 1px solid #e5e7eb; page-break-inside: avoid;">
            <td style="padding: 10px 12px; vertical-align: top; color: #111827; font-weight: 500;">
                {material_name_esc}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: #111827;">
                {code_concat_esc}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: #6b7280;">
                {atm_number_esc}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: #6b7280;">
                {created_str}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: #559FB8; font-weight: 600;">
                {loans_count}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: {color_loan_days}; font-weight: 600;">
                {loan_days_str}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: {color_delay}; font-weight: 600;">
                {delay_str}
            </td>
            <td style="padding: 10px 12px; vertical-align: top; color: {color_maint}; font-weight: 600;">
                {maint_str}
            </td>
        </tr>
        """
        obs = html_lib.escape(item.catalog.asset.asset_description) if item.catalog.asset.asset_description else ""
        
        # ATENÇÃO: colspan alterado para 8!
        row += f"""
        <tr style="border-bottom: 1px solid #e5e7eb; background-color: #f9fafb;">
            <td colspan="8" style="padding: 4px 12px 16px 12px; font-size: 9px; color: #6b7280; font-style: italic;">
                <strong>Descrição:</strong> {obs}
            </td>
        </tr>
        """
        rows_html.append(row)

    # Une todas as linhas
    loans_tbody_html = "".join(rows_html)

    # Se a lista estiver vazia por algum motivo
    if not loans_tbody_html:
        loans_tbody_html = """
        <tr>
            <td colspan="5" style="padding: 20px; text-align: center; color: #6b7280; font-style: italic;">
                Nenhum item encontrado.
            </td>
        </tr>
        """

    # ---------- HTML FINAL (ESTRUTURA EM TABELAS) ----------
    ASSETS_DIR = (Path(__file__).resolve().parent.parent / "assets" ).resolve()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    html = f"""
    <div
        style="
            position: relative;
            width: 100%;
            min-height: 297mm;
            box-sizing: border-box;
            background-color: #f9fafb;
            overflow: hidden;
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

        <div style="padding: 0 26px 36px 26px; padding-bottom: 60px;">
            <section style="display: block; width: 100%; margin-top: 10px; margin-bottom: 20px;">
    
                <h3 style="margin: 0 0 15px 0; font-size: 15px; font-weight: 700; color: #111827; border-bottom: 2px solid #e5e7eb; padding-bottom: 8px;">
                    INVENTÁRIO AUDIOVISUAL 
                </h3>

                <div style="
                    background-color: #ffffff;
                    border: 1px solid #e5e7eb;
                    border-radius: 6px;
                    overflow: hidden;
                ">
                    <table style="width: 100%; border-collapse: collapse; font-size: 10px;">
                        <thead>
                            <tr style="background-color: #f3f4f6; border-bottom: 2px solid #e5e7eb;">
                                <th style="padding: 10px 12px; text-align: left; width: 25%; color: #374151; font-weight: 700;">NOME</th>
                                <th style="padding: 10px 12px; text-align: left; width: 14%; color: #374151; font-weight: 700;">CÓDIGO</th>
                                <th style="padding: 10px 12px; text-align: left; width: 10%; color: #374151; font-weight: 700;">ATM</th>
                                <th style="padding: 10px 12px; text-align: left; width: 15%; color: #374151; font-weight: 700;">CADASTRO EM</th>
                                <th style="padding: 10px 12px; text-align: left; width: 9%; color: #374151; font-weight: 700;">USOS</th>
                                <th style="padding: 10px 12px; text-align: left; width: 9%; color: #374151; font-weight: 700;">USO (DIAS)</th>
                                <th style="padding: 10px 12px; text-align: left; width: 9%; color: #374151; font-weight: 700;">ATRASO (DIAS)</th>
                                <th style="padding: 10px 12px; text-align: left; width: 9%; color: #374151; font-weight: 700;">MANUT. (DIAS)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {loans_tbody_html}
                        </tbody>
                    </table>
                </div>
            </section>
        </div>

        <div 
            style="
                position: absolute;
                bottom: 0;
                left: 0;
                right: 0;
                height: 50px;
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
                        Página 1
                    </td>
                </tr>
              </table>
          </div>
        </div>
    </div>
"""
    return html

def render_loan_terms(item, loan) -> str:
    """
    Gera um HTML estilizado de um Termo de Cessão de Uso de Bem Móvel.
    Preenche automaticamente os dados do empréstimo e do item.
    """

    # --- 1. DADOS DO ITEM (ANEXO I) ---
    material_name = getattr(item.catalog.asset.material, "material_name", "")
    asset_description = getattr(item.catalog.asset, "asset_description", "")
    
    code_concat = ""
    try:
        asset_code = item.catalog.asset.asset_code or ""
        asset_check_digit = item.catalog.asset.asset_check_digit or ""
        code_concat = f"{asset_code}-{asset_check_digit}" if asset_check_digit else asset_code
    except AttributeError:
        code_concat = getattr(item, "asset_code", "") or ""

    atm_number = getattr(item.catalog.asset, "atm_number", getattr(item, "atm_number", ""))
    
    item_full_description = f"{material_name} - {asset_description}".strip(" -")
    if code_concat:
        item_full_description += f". Código do Bem: {code_concat}"
    if atm_number and str(atm_number).lower() != "none":
        item_full_description += f". ATM: {atm_number}"
        
    item_full_description_esc = html_lib.escape(item_full_description)

    # --- 2. DADOS DO EMPRÉSTIMO ---
    # Cessionário (Priorizamos o guardião temporário, ou o solicitante se não houver)
    cessionario = loan.temporary_guardian or loan.requester
    cessionario_name = getattr(cessionario, 'username', getattr(cessionario, 'email', '_________________________________'))
    cessionario_name_esc = html_lib.escape(cessionario_name)

    # Datas e Prazos
    start_date = loan.start_at
    end_date = loan.end_at
    
    if start_date and end_date:
        # Pega a diferença em dias ignorando as horas
        prazo_dias = (end_date.replace(tzinfo=None).date() - start_date.replace(tzinfo=None).date()).days
        prazo_dias_str = str(prazo_dias)
    else:
        prazo_dias_str = "_______"

    start_date_str = start_date.strftime('%d/%m/%Y') if start_date else "_____/_____/_______"
    

    finalidade_esc = "_______________________________________________________________________________________________________________________________"
    
    # Data da Assinatura (Hoje)
    hoje = datetime.now()
    meses = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"]
    dia_hoje = hoje.strftime("%d")
    mes_hoje = meses[hoje.month - 1]
    ano_hoje = hoje.strftime("%Y")


    # --- 3. HTML FINAL DO DOCUMENTO ---

    ASSETS_DIR = (Path(__file__).resolve().parent.parent / "assets" ).resolve()
    EE_LOGO_URI = (ASSETS_DIR / "ee_logo.png").resolve().as_uri()
    SP_LOGO_URI = (ASSETS_DIR / "sp_logo.png").resolve().as_uri()

    html = f"""
    <div
        style="
            position: relative;
            width: 100%;
            height: 297mm;
            box-sizing: border-box;
            background-color: #ffffff;
            overflow: hidden;
            font-family: 'Lexend', sans-serif;
            font-size: 11px;
            color: #000000;
        "
    >
        <table
            style="
                width: 100%;
                border-collapse: collapse;
                margin: 0;
                padding: 40px;
                padding-bottom: 0;
            "
        >
            <tr>
                <td style="width: 150px; padding: 20px 48px 10px 48px; vertical-align: middle;">
                    <img src="{SP_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                </td>
                <td style="width: 150px; padding: 20px 48px 10px 48px; text-align: right; vertical-align: middle;">
                    <img src="{EE_LOGO_URI}" style="height: 48px; max-width: 120px; object-fit: contain;" />
                </td>
            </tr>
        </table>

        <div style="padding: 0 40px 20px 40px;">
            <h2
              style="
                font-size: 18px;
                font-weight: 700;
                margin-bottom: 16px;
                text-align: center;
              "
            >
              TERMO DE CESSÃO DE USO DE BEM MÓVEL
            </h2>

            <div style="text-align: justify; line-height: 1.5; font-size: 11px;">
                <p style="margin-bottom: 10px;">
                    A UNIVERSIDADE FEDERAL DE MINAS GERAIS, por intermédio da Escola de Engenharia,
                    doravante designada UFMG, e, <strong>{cessionario_name_esc}</strong>, domiciliado(a) na
                    _____________________________________________________ no __________, bairro: ___________________, cidade:
                    __________________________, portador(a) do CPF: ________________________, CI:
                    ________________________, celular: ____________________________ doravante
                    designado <strong>CESSIONÁRIO</strong>, firmam o presente termo de cessão de uso de bem(ns) móvel(is),
                    que se regerá pelas cláusulas e condições seguintes:
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>1</strong> - O presente termo tem por objeto a cessão gratuita de uso de bem(ns) móvel(is),
                    relacionado(s) no Anexo I, parte integrante deste instrumento, doravante designado objeto da
                    cessão de uso, pertencente à UFMG em favor do CESSIONÁRIO, transferindo-lhe, por
                    conseguinte, em caráter provisório, a sua posse e a responsabilidade.
                </p>

                <p style="margin-bottom: 15px;">
                    <strong>2</strong> - A presente cessão de uso tem como finalidade(s):
                </p>
                <p style="margin-top: 10px;">
                    {finalidade_esc}
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>3</strong> - Ao objeto da cessão de uso não poderá ser dada destinação diversa daquela, sob pena
                    de rescisão e perdas e danos.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>4</strong> - O presente termo de cessão de uso vigorará pelo prazo de <strong>{prazo_dias_str}</strong> dias, contados
                    a partir de <strong>{start_date_str}</strong>.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>5</strong> - As despesas decorrentes da retirada e devolução do objeto da cessão de uso, bem como
                    todas aquelas inerentes a sua manutenção e conservação correrão por conta do
                    CESSIONÁRIO, incumbindo-lhe, ainda, nas mesmas condições, a sua guarda até a efetiva
                    devolução.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>6</strong> - Finda, a qualquer tempo, a cessão de uso, deverá o CESSIONÁRIO restituir o objeto da
                    cessão de uso em perfeitas condições de uso e conservação, salvo as deteriorações
                    decorrente do seu uso normal.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>7</strong> - Caso seja verificado qualquer dano ao objeto da cessão de uso que não decorra de
                    deteriorações do uso normal, poderá a UFMG exigir a reposição das partes danificadas ou o
                    pagamento do valor correspondente ao prejuízo em dinheiro.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>8</strong> - A devolução será formalizada por meio de baixa no presente termo de cessão de uso.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>9</strong> - O descumprimento, pelo CESSIONÁRIO, de qualquer de suas obrigações dará a UFMG
                    o direito de considerar rescindida de pleno direito a presente cessão e exigir a reparação de
                    danos.
                </p>

                <p style="margin-bottom: 10px;">
                    <strong>10</strong> - Será considerado descumprimento das condições avençadas, para fins de rescisão, o
                    mau uso do objeto da cessão de uso, a alteração de sua destinação.
                </p>

                <p style="margin-bottom: 10px;">
                    E assim, por estarem de acordo, assinam o presente termo em 02 (duas) vias de igual teor,
                    na presença de testemunhas.<br><br>
                    Belo Horizonte, {dia_hoje} de {mes_hoje} de {ano_hoje}.
                </p>
                
                <h3 style="font-size: 13px; font-weight: 700; margin-bottom: 10px;">
                    Descrição do bem objeto do termo de cessão de uso: 
                </h3>
                <p style="margin-bottom: 10px; padding: 10px; background-color: #f9fafb; border: 1px solid #e5e7eb; border-radius: 4px;">
                    {item_full_description_esc}
                </p>

                <table style="width: 100%; border-collapse: collapse; text-align: center; font-size: 12px; margin-top: 40px;">
                    <tr>
                        <td style="width: 50%; padding: 10px;">
                            ______________________________________________________<br>
                            <strong>UFMG</strong><br>
                            Siape:........................................
                        </td>
                        <td style="width: 50%; padding: 10px;">
                            ______________________________________________________<br>
                            <strong>CESSIONÁRIO</strong><br>
                            CPF:........................................
                        </td>
                    </tr>
                </table>

                <p><strong>Testemunhas:</strong></p>
                <table style="width: 100%; border-collapse: collapse; text-align: center; font-size: 12px; margin-top: 10px; margin-bottom: 10px;">
                    <tr>
                        <td style="width: 50%; padding: 10px;">
                            ______________________________________________________<br>
                            Nome:...............................................................<br>
                            CPF:.................................................................
                        </td>
                        <td style="width: 50%; padding: 10px;">
                            ______________________________________________________<br>
                            Nome:...............................................................<br>
                            CPF:.................................................................
                        </td>
                    </tr>
                </table>

                <div 
                    style="
                        position: absolute;
                        bottom: 0;
                        left: 0;
                        right: 0;
                        height: 50px;
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
                                    Página 1 de 1
                                </td>
                            </tr>
                        </table>
                    </div>
                </div>
                

                <div style="border: 1px solid #e5e7eb; padding: 15px; margin-top: 25px; border-radius: 4px; background-color:#f9fafb; page-break-before: always;">
                    <p style="margin-bottom: 25px; font-weight: 700;">BAIXA / ENTREGA do objeto da cessão de uso: _____/__________/______.</p>
                    <p style="margin-bottom: 15px;">Conferência do bem: ____________________________________________________________________</p>
                    <p style="margin-bottom: 5px;">Nome:...................................................................................................................................................</p>
                    <p style="margin: 0;">Siape:.............................................................</p>
                </div>

            </div>
        </div>
    </div>
"""
    return html