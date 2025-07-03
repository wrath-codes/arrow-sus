use super::{GroupInfo, SubsystemInfo};

pub const CIH: SubsystemInfo = SubsystemInfo {
    code: "CIH",
    name: "Comunicação de Internação Hospitalar",
    description: Some("Base de dados da Comunicação de Internação Hospitalar (CIH)."),
    long_description: Some(
        "A Comunicação de Internação Hospitalar (CIH) é utilizada para registrar informações \
referentes às internações hospitalares realizadas no âmbito do SUS, com o objetivo de subsidiar \
o planejamento, regulação, avaliação e financiamento da assistência hospitalar.",
    ),
    url: Some("https://datasus.saude.gov.br/"), // Replace with official CIH page if available
    groups: &[GroupInfo {
        code: "CR",
        name: "Comunicação de Internação Hospitalar",
        description: Some("Registros mensais de internações hospitalares comunicadas."),
        long_description: None,
        url: Some("https://datasus.saude.gov.br/"),
    }],
};
