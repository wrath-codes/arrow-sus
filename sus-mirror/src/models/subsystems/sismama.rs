use super::{GroupInfo, SubsystemInfo};

pub const SISMAMA: SubsystemInfo = SubsystemInfo {
    code: "SISMAMA",
    name: "Sistema de Informações de Cânceres de Mama",
    description: Some("Base de dados do Sistema de Informações de Cânceres de Mama (SISMAMA)."),
    long_description: Some(
        "O SISMAMA integra o Sistema de Informação do Câncer de Mama, armazenando informações \
sobre exames clínicos, mamografias e exames histopatológicos, auxiliando no rastreamento, \
diagnóstico precoce e controle do câncer de mama no Brasil.",
    ),
    url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-de-mama"),
    groups: &[
        GroupInfo {
            code: "CM",
            name: "Coleta Mamográfica",
            description: Some("Exames de rastreamento mamográfico."),
            long_description: None,
            url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-de-mama"),
        },
        GroupInfo {
            code: "HM",
            name: "Histopatológico de Mama",
            description: Some("Exames histopatológicos confirmatórios para câncer de mama."),
            long_description: None,
            url: Some("https://www.gov.br/inca/pt-br/assuntos/controle-do-cancer-de-mama"),
        },
    ],
};
