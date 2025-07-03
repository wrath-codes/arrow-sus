use super::{GroupInfo, SubsystemInfo};

pub const SINASC: SubsystemInfo = SubsystemInfo {
    code: "SINASC",
    name: "Sistema de Informações sobre Nascidos Vivos",
    description: Some("Base de dados do Sistema de Informações sobre Nascidos Vivos (SINASC)."),
    long_description: Some(
        "O Sistema de Informações sobre Nascidos Vivos (SINASC) é o sistema oficial de registro \
das Declarações de Nascidos Vivos no Brasil. Ele possibilita acompanhar indicadores de natalidade, \
perfil de saúde materno-infantil, características demográficas, além de subsidiar políticas públicas \
voltadas à saúde da mulher e da criança.",
    ),
    url: Some("http://sinasc.saude.gov.br/"),
    groups: &[
        GroupInfo {
            code: "DN",
            name: "Declarações de Nascidos Vivos",
            description: Some("Declarações individuais de nascimentos registrados."),
            long_description: None,
            url: Some("http://sinasc.saude.gov.br/"),
        },
        GroupInfo {
            code: "DNR",
            name: "Dados dos Nascidos Vivos por UF de residência",
            description: Some("Registros agregados por UF de residência."),
            long_description: None,
            url: Some("http://sinasc.saude.gov.br/"),
        },
    ],
};
