use super::{GroupInfo, SubsystemInfo};

pub const SISCOLO: SubsystemInfo = SubsystemInfo {
    code: "SISCOLO",
    name: "Sistema de Informações de Cânceres de Colo de Útero",
    description: Some(
        "Base de dados do Sistema de Informações de Cânceres de Colo de Útero (SISCOLO).",
    ),
    long_description: Some(
        "O SISCOLO integra o Sistema de Câncer do Colo do Útero, armazenando informações sobre \
exames citológicos e histopatológicos, contribuindo para o rastreamento, diagnóstico precoce \
e controle do câncer do colo do útero no Brasil.",
    ),
    url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-do-colo-do-utero"),
    groups: &[
        GroupInfo {
            code: "CC",
            name: "Coleta Citológica",
            description: Some("Exames citopatológicos de rastreamento de câncer de colo do útero."),
            long_description: None,
            url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-do-colo-do-utero"),
        },
        GroupInfo {
            code: "HC",
            name: "Histopatológico",
            description: Some("Exames histopatológicos confirmatórios."),
            long_description: None,
            url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-do-colo-do-utero"),
        },
    ],
};
