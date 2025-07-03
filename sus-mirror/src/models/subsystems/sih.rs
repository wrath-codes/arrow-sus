use super::{GroupInfo, SubsystemInfo};

pub const SIHSUS: SubsystemInfo = SubsystemInfo {
    code: "SIHSUS",
    name: "Sistema de Informações Hospitalares",
    description: Some("Base de dados do Sistema de Informação Hospitalar do SUS (SIHSUS)."),
    long_description: Some(
        "A finalidade do AIH (Sistema SIHSUS) é transcrever todos os atendimentos provenientes \
de internações hospitalares financiadas pelo SUS e, após processamento, gerar relatórios para \
os gestores que possibilitem o pagamento dos estabelecimentos de saúde. Além disso, o nível \
Federal recebe mensalmente uma base de dados de todas as internações autorizadas (aprovadas \
ou não para pagamento) para que possam ser repassados às Secretarias de Saúde os valores de \
Produção de Média e Alta complexidade, além dos valores de CNRAC, FAEC e de Hospitais \
Universitários em suas variadas formas de contrato de gestão.",
    ),
    url: Some(
        "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
    ),
    groups: &[
        GroupInfo {
            code: "RD",
            name: "AIH Reduzida",
            description: Some("Autorizações de Internação Hospitalar (AIH) em formato reduzido."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            ),
        },
        GroupInfo {
            code: "RJ",
            name: "AIH Rejeitada",
            description: Some("Autorizações de Internação Hospitalar rejeitadas."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            ),
        },
        GroupInfo {
            code: "ER",
            name: "AIH Rejeitada com Erro",
            description: Some("AIHs rejeitadas por erro de processamento."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            ),
        },
        GroupInfo {
            code: "SP",
            name: "Serviços Profissionais",
            description: Some("Serviços profissionais vinculados às AIHs."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            ),
        },
        GroupInfo {
            code: "CH",
            name: "Cadastro Hospitalar",
            description: Some("Informações cadastrais dos hospitais."),
            long_description: None,
            url: Some(
                "https://datasus.saude.gov.br/acesso-a-informacao/morbidade-hospitalar-do-sus-sih-sus/",
            ),
        },
        GroupInfo {
            code: "CM",
            name: "Grupo não documentado",
            description: Some("TODO: Revisar definição do grupo CM."),
            long_description: None,
            url: None,
        },
    ],
};
