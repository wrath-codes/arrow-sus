use super::{GroupInfo, SubsystemInfo};

pub const CNES: SubsystemInfo = SubsystemInfo {
    code: "CNES",
    name: "Cadastro Nacional de Estabelecimentos de Saúde",
    description: Some("Cadastro oficial de todos os estabelecimentos de saúde no Brasil."),
    long_description: Some(
        "O Cadastro Nacional de Estabelecimentos de Saúde (CNES) é o sistema de informação oficial \
de cadastramento de informações de todos os estabelecimentos de saúde no país, independentemente \
de sua natureza jurídica ou de integrarem o Sistema Único de Saúde (SUS). Trata-se do cadastro oficial \
do Ministério da Saúde (MS) no tocante à realidade da capacidade instalada e mão-de-obra assistencial \
de saúde no Brasil em estabelecimentos de saúde públicos ou privados, com convênio SUS ou não.",
    ),
    url: Some("https://cnes.datasus.gov.br/"),
    groups: &[
        GroupInfo {
            code: "DC",
            name: "Dados Complementares",
            description: Some("Dados complementares dos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "EE",
            name: "Estabelecimento de Ensino",
            description: Some("Informações sobre estabelecimentos de ensino em saúde."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "EF",
            name: "Estabelecimento Filantrópico",
            description: Some("Informações sobre estabelecimentos filantrópicos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "EP",
            name: "Equipes",
            description: Some("Equipes registradas nos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "EQ",
            name: "Equipamentos",
            description: Some("Inventário de equipamentos dos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "GM",
            name: "Gestão e Metas",
            description: Some("Informações de gestão e metas."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "HB",
            name: "Habilitação",
            description: Some("Habilitações específicas dos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "IN",
            name: "Incentivos",
            description: Some("Incentivos recebidos pelos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "LT",
            name: "Leitos",
            description: Some("Leitos disponíveis nos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "PF",
            name: "Profissional",
            description: Some("Profissionais registrados nos estabelecimentos."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "RC",
            name: "Regra Contratual",
            description: Some("Regras contratuais aplicáveis."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "SR",
            name: "Serviço Especializado",
            description: Some("Serviços especializados prestados."),
            long_description: None,
            url: None,
        },
        GroupInfo {
            code: "ST",
            name: "Estabelecimentos",
            description: Some("Informações gerais dos estabelecimentos."),
            long_description: None,
            url: None,
        },
    ],
};
