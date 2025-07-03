use super::{GroupInfo, SubsystemInfo};

pub const CIHA: SubsystemInfo = SubsystemInfo {
    code: "CIHA",
    name: "Comunicação de Internação Hospitalar e Ambulatorial",
    description: Some(
        "Amplia o processo de planejamento, programação, controle e avaliação da assistência à saúde.",
    ),
    long_description: Some(
        "A CIHA foi criada para ampliar o processo de planejamento, programação, controle, \
    avaliação e regulação da assistência à saúde permitindo um conhecimento mais abrangente e profundo \
    dos perfis nosológico e epidemiológico da população brasileira, da capacidade instalada e do \
    potencial de produção de serviços do conjunto de estabelecimentos de saúde do País. O sistema \
    permite o acompanhamento das ações e serviços de saúde custeados por: planos privados de assistência \
    à saúde; planos públicos; pagamento particular por pessoa física; pagamento particular por pessoa \
    jurídica; programas e projetos federais (PRONON, PRONAS, PROADI); recursos próprios das secretarias \
    municipais e estaduais de saúde; DPVAT; gratuidade e, a partir da publicação da Portaria GM/MS nº \
    2.905/2022, consórcios públicos. As informações registradas na CIHA servem como base para o processo \
    de Certificação de Entidades Beneficentes de Assistência Social em Saúde (CEBAS) e para monitoramento \
    dos programas PRONAS e PRONON.",
    ),
    url: Some("http://ciha.datasus.gov.br/CIHA/index.php"),
    groups: &[GroupInfo {
        code: "CIHA",
        name: "Comunicação de Internação Hospitalar e Ambulatorial",
        description: Some("Comunicação de Internação Hospitalar e Ambulatorial."),
        long_description: None,
        url: Some("http://ciha.datasus.gov.br/CIHA/index.php"),
    }],
};
