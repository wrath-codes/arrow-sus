use super::{GroupInfo, SubsystemInfo};

pub const RESP: SubsystemInfo = SubsystemInfo {
    code: "RESP",
    name: "Registro de Eventos em Saúde Pública",
    description: Some("Base de dados do Registro de Eventos em Saúde Pública (RESP)."),
    long_description: Some(
        "O Registro de Eventos em Saúde Pública (RESP) é um sistema para o registro e monitoramento \
de casos suspeitos ou confirmados de eventos de importância para a saúde pública, como a Síndrome \
Congênita do Zika (SCZ) e outras emergências sanitárias.",
    ),
    url: Some("https://www.gov.br/saude/pt-br/composicao/svsa/vigilancia-epidemiologica/resp"), // or best public page
    groups: &[GroupInfo {
        code: "RESP",
        name: "Notificações de Casos Suspeitos",
        description: Some("Registros de notificações de casos suspeitos de SCZ e outros eventos."),
        long_description: None,
        url: Some("https://www.gov.br/saude/pt-br/composicao/svsa/vigilancia-epidemiologica/resp"),
    }],
};
