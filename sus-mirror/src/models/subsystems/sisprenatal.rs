use super::{GroupInfo, SubsystemInfo};

pub const SISPRENATAL: SubsystemInfo = SubsystemInfo {
    code: "SISPRENATAL",
    name: "Sistema de Monitoramento e Avaliação do Pré-Natal, Parto, Puerpério e Criança",
    description: Some(
        "Base de dados do Sistema de Monitoramento e Avaliação do Pré-Natal (SISPRENATAL).",
    ),
    long_description: Some(
        "O SISPRENATAL foi desenvolvido para monitorar, avaliar e qualificar a assistência \
pré-natal, parto, puerpério e atenção à saúde da criança. Permite o acompanhamento de gestantes, \
recém-nascidos e puérperas, subsidiando o planejamento de ações de saúde e a melhoria da qualidade \
dos serviços prestados.",
    ),
    url: Some("https://www.gov.br/saude/pt-br/composicao/saes/sas/dapes/saude-da-mulher"),
    groups: &[GroupInfo {
        code: "PN",
        name: "Pré-Natal",
        description: Some(
            "Dados de acompanhamento do pré-natal, parto, puerpério e saúde da criança.",
        ),
        long_description: None,
        url: Some("https://www.gov.br/saude/pt-br/composicao/saes/sas/dapes/saude-da-mulher"),
    }],
};
