use super::{GroupInfo, SubsystemInfo};

pub const IBGE: SubsystemInfo = SubsystemInfo {
    code: "IBGE",
    name: "IBGE DataSUS",
    description: Some(
        "População Residente, Censos, Contagens Populacionais e Projeções Intercensitárias.",
    ),
    long_description: Some(
        "São apresentados informações sobre a população residente, estratificadas por município, \
faixas etárias e sexo, obtidas a partir dos Censos Demográficos, Contagens Populacionais \
e Projeções Intercensitárias, disponibilizados no repositório do DATASUS.",
    ),
    url: Some("ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE"),
    groups: &[
        GroupInfo {
            code: "POP",
            name: "População Residente",
            description: Some(
                "Estatísticas da população residente por município, faixa etária e sexo.",
            ),
            long_description: None,
            url: Some("ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/POP"),
        },
        GroupInfo {
            code: "CENSO",
            name: "Censos Demográficos",
            description: Some("Dados dos Censos Demográficos do IBGE."),
            long_description: None,
            url: Some("ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/censo"),
        },
        GroupInfo {
            code: "POPTCU",
            name: "População TCU",
            description: Some("Estimativas populacionais conforme TCU."),
            long_description: None,
            url: Some("ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/POPTCU"),
        },
        GroupInfo {
            code: "PROJPOP",
            name: "Projeções Intercensitárias",
            description: Some("Projeções populacionais intercensitárias."),
            long_description: None,
            url: Some("ftp://ftp.datasus.gov.br/dissemin/publicos/IBGE/projpop"),
        },
    ],
};
