use super::{GroupInfo, SubsystemInfo};

pub const SIASUS: SubsystemInfo = SubsystemInfo {
    code: "SIA",
    name: "Sistema de Informações Ambulatoriais",
    description: Some("Base de dados do Sistema de Informação Ambulatorial do SUS (SIA-SUS)."),
    long_description: Some(
        "O Sistema de Informação Ambulatorial (SIA) foi instituído pela Portaria GM/MS n.º 896 \
        de 29 de junho de 1990. Originalmente concebido a partir do projeto SICAPS (Sistema de Informação \
        e Controle Ambulatorial da Previdência Social), o SIA herdou conceitos, objetivos e diretrizes \
        essenciais para seu desenvolvimento, como: (i) o acompanhamento das programações físicas e \
        orçamentárias; (ii) o monitoramento das ações de saúde produzidas; (iii) a agilização do pagamento \
        e controle orçamentário e financeiro; e (iv) a formação de banco de dados para contribuir com \
        a construção do SUS.",
    ),
    url: Some("http://sia.datasus.gov.br/principal/index.php"),
    groups: &[
        GroupInfo {
            code: "AB",
            name: "APAC de Cirurgia Bariátrica",
            description: Some("Autorização para procedimentos de cirurgia bariátrica."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "ABO",
            name: "APAC de Acompanhamento Pós Cirurgia Bariátrica",
            description: Some("Acompanhamento ambulatorial pós cirurgia bariátrica."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "ACF",
            name: "APAC de Confecção de Fístula",
            description: Some("Autorização para confecção de fístula."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AD",
            name: "APAC de Laudos Diversos",
            description: Some("APAC para procedimentos diversos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AM",
            name: "APAC de Medicamentos",
            description: Some("Autorização para fornecimento de medicamentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AMP",
            name: "APAC de Acompanhamento Multiprofissional",
            description: Some("Acompanhamento multiprofissional ambulatorial."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AN",
            name: "APAC de Nefrologia",
            description: Some("Tratamentos nefrológicos autorizados via APAC."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AQ",
            name: "APAC de Quimioterapia",
            description: Some("Tratamentos de quimioterapia."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "AR",
            name: "APAC de Radioterapia",
            description: Some("Tratamentos de radioterapia."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "ATD",
            name: "APAC de Tratamento Dialítico",
            description: Some("Tratamento de diálise renal."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "BI",
            name: "Boletim de Produção Ambulatorial Individualizado",
            description: Some("BPA Individualizado."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "PA",
            name: "Produção Ambulatorial",
            description: Some("Dados gerais de produção ambulatorial."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "PS",
            name: "RAAS Psicossocial",
            description: Some("Registro das Ações Ambulatoriais de Saúde Psicossocial."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "SAD",
            name: "RAAS de Atenção Domiciliar",
            description: Some("Registro das Ações Ambulatoriais de Atenção Domiciliar."),
            long_description: None,
            url: None,
        },
    ],
};
