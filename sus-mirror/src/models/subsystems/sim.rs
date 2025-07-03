use super::{GroupInfo, SubsystemInfo};

pub const SIM: SubsystemInfo = SubsystemInfo {
    code: "SIM",
    name: "Sistema de Informação sobre Mortalidade",
    description: Some("Base de dados do Sistema de Informação sobre Mortalidade (SIM)."),
    long_description: Some(
        "O Sistema de Informação sobre Mortalidade (SIM) é a base oficial de registro das Declarações \
de Óbito no Brasil. Permite acompanhar tendências de mortalidade, causas de óbito e subsidiar \
ações de vigilância em saúde e políticas públicas voltadas à redução da mortalidade evitável. \
A base inclui registros em formato CID-9 (histórico) e CID-10 (atual), com subdivisões como \
óbito fetal, materno e infantil para análises específicas.",
    ),
    url: Some("http://sim.saude.gov.br"),
    groups: &[
        GroupInfo {
            code: "DO",
            name: "Declaração de Óbito",
            description: Some("Registros gerais de óbitos."),
            long_description: None,
            url: Some("http://sim.saude.gov.br"),
        },
        GroupInfo {
            code: "DOFET",
            name: "Óbito Fetal",
            description: Some("Declarações de óbito fetal."),
            long_description: None,
            url: Some("http://sim.saude.gov.br"),
        },
        GroupInfo {
            code: "DOINF",
            name: "Óbito Infantil",
            description: Some("Declarações de óbito infantil."),
            long_description: None,
            url: Some("http://sim.saude.gov.br"),
        },
        GroupInfo {
            code: "DOMAT",
            name: "Óbito Materno",
            description: Some("Declarações de óbito materno."),
            long_description: None,
            url: Some("http://sim.saude.gov.br"),
        },
        GroupInfo {
            code: "DOREXT",
            name: "Óbito Externo",
            description: Some("Declarações de óbito por causas externas."),
            long_description: None,
            url: Some("http://sim.saude.gov.br"),
        },
    ],
};
