use std::sync::LazyLock;

use fluent::{concurrent::FluentBundle, FluentArgs, FluentResource, FluentValue};
use include_flate::flate;
use sys_locale::get_locale;
use tracing::{error, info, warn};
use unic_langid::LanguageIdentifier;

flate!(static EN_US_FTL: str from "i18n/en-US.ftl");
flate!(static DE_DE_FTL: str from "i18n/de-DE.ftl");
flate!(static PT_PT_FTL: str from "i18n/pt-PT.ftl");
flate!(static PT_BR_FTL: str from "i18n/pt-BR.ftl");
flate!(static FR_FR_FTL: str from "i18n/fr-FR.ftl");
flate!(static ES_ES_FTL: str from "i18n/es-ES.ftl");
flate!(static IT_IT_FTL: str from "i18n/it-IT.ftl");
flate!(static NL_NL_FTL: str from "i18n/nl-NL.ftl");
flate!(static SV_SE_FTL: str from "i18n/sv-SE.ftl");
flate!(static DA_DK_FTL: str from "i18n/da-DK.ftl");
flate!(static NB_NO_FTL: str from "i18n/nb-NO.ftl");
flate!(static FI_FI_FTL: str from "i18n/fi-FI.ftl");
flate!(static IS_IS_FTL: str from "i18n/is-IS.ftl");
flate!(static GA_IE_FTL: str from "i18n/ga-IE.ftl");
flate!(static MT_MT_FTL: str from "i18n/mt-MT.ftl");
flate!(static RU_RU_FTL: str from "i18n/ru-RU.ftl");
flate!(static UK_UA_FTL: str from "i18n/uk-UA.ftl");
flate!(static PL_PL_FTL: str from "i18n/pl-PL.ftl");
flate!(static CS_CZ_FTL: str from "i18n/cs-CZ.ftl");
flate!(static SK_SK_FTL: str from "i18n/sk-SK.ftl");
flate!(static SL_SI_FTL: str from "i18n/sl-SI.ftl");
flate!(static HR_HR_FTL: str from "i18n/hr-HR.ftl");
flate!(static SR_RS_FTL: str from "i18n/sr-RS.ftl");
flate!(static BS_BA_FTL: str from "i18n/bs-BA.ftl");
flate!(static RO_RO_FTL: str from "i18n/ro-RO.ftl");
flate!(static BG_BG_FTL: str from "i18n/bg-BG.ftl");
flate!(static HU_HU_FTL: str from "i18n/hu-HU.ftl");
flate!(static EL_GR_FTL: str from "i18n/el-GR.ftl");
flate!(static TR_TR_FTL: str from "i18n/tr-TR.ftl");
flate!(static ET_EE_FTL: str from "i18n/et-EE.ftl");
flate!(static LV_LV_FTL: str from "i18n/lv-LV.ftl");
flate!(static LT_LT_FTL: str from "i18n/lt-LT.ftl");
flate!(static SQ_AL_FTL: str from "i18n/sq-AL.ftl");
flate!(static MK_MK_FTL: str from "i18n/mk-MK.ftl");
flate!(static AR_SA_FTL: str from "i18n/ar-SA.ftl");
flate!(static ZH_CN_FTL: str from "i18n/zh-CN.ftl");
flate!(static ZH_TW_FTL: str from "i18n/zh-TW.ftl");
flate!(static JA_JP_FTL: str from "i18n/ja-JP.ftl");
flate!(static KO_KR_FTL: str from "i18n/ko-KR.ftl");
flate!(static HI_IN_FTL: str from "i18n/hi-IN.ftl");
flate!(static ID_ID_FTL: str from "i18n/id-ID.ftl");

struct LanguageSelector<'a> {
    code: &'a str,
    ftl: &'static LazyLock<String>,
}

static SUPPORTED_LOCALES: [LanguageSelector; 41] = [
    LanguageSelector {
        code: "en-US",
        ftl: &EN_US_FTL,
    },
    LanguageSelector {
        code: "de-DE",
        ftl: &DE_DE_FTL,
    },
    LanguageSelector {
        code: "pt-PT",
        ftl: &PT_PT_FTL,
    },
    LanguageSelector {
        code: "pt-BR",
        ftl: &PT_BR_FTL,
    },
    LanguageSelector {
        code: "fr-FR",
        ftl: &FR_FR_FTL,
    },
    LanguageSelector {
        code: "es-ES",
        ftl: &ES_ES_FTL,
    },
    LanguageSelector {
        code: "it-IT",
        ftl: &IT_IT_FTL,
    },
    LanguageSelector {
        code: "nl-NL",
        ftl: &NL_NL_FTL,
    },
    LanguageSelector {
        code: "sv-SE",
        ftl: &SV_SE_FTL,
    },
    LanguageSelector {
        code: "da-DK",
        ftl: &DA_DK_FTL,
    },
    LanguageSelector {
        code: "nb-NO",
        ftl: &NB_NO_FTL,
    },
    LanguageSelector {
        code: "fi-FI",
        ftl: &FI_FI_FTL,
    },
    LanguageSelector {
        code: "is-IS",
        ftl: &IS_IS_FTL,
    },
    LanguageSelector {
        code: "ga-IE",
        ftl: &GA_IE_FTL,
    },
    LanguageSelector {
        code: "mt-MT",
        ftl: &MT_MT_FTL,
    },
    LanguageSelector {
        code: "ru-RU",
        ftl: &RU_RU_FTL,
    },
    LanguageSelector {
        code: "uk-UA",
        ftl: &UK_UA_FTL,
    },
    LanguageSelector {
        code: "pl-PL",
        ftl: &PL_PL_FTL,
    },
    LanguageSelector {
        code: "cs-CZ",
        ftl: &CS_CZ_FTL,
    },
    LanguageSelector {
        code: "sk-SK",
        ftl: &SK_SK_FTL,
    },
    LanguageSelector {
        code: "sl-SI",
        ftl: &SL_SI_FTL,
    },
    LanguageSelector {
        code: "hr-HR",
        ftl: &HR_HR_FTL,
    },
    LanguageSelector {
        code: "sr-RS",
        ftl: &SR_RS_FTL,
    },
    LanguageSelector {
        code: "bs-BA",
        ftl: &BS_BA_FTL,
    },
    LanguageSelector {
        code: "ro-RO",
        ftl: &RO_RO_FTL,
    },
    LanguageSelector {
        code: "bg-BG",
        ftl: &BG_BG_FTL,
    },
    LanguageSelector {
        code: "hu-HU",
        ftl: &HU_HU_FTL,
    },
    LanguageSelector {
        code: "el-GR",
        ftl: &EL_GR_FTL,
    },
    LanguageSelector {
        code: "tr-TR",
        ftl: &TR_TR_FTL,
    },
    LanguageSelector {
        code: "et-EE",
        ftl: &ET_EE_FTL,
    },
    LanguageSelector {
        code: "lv-LV",
        ftl: &LV_LV_FTL,
    },
    LanguageSelector {
        code: "lt-LT",
        ftl: &LT_LT_FTL,
    },
    LanguageSelector {
        code: "sq-AL",
        ftl: &SQ_AL_FTL,
    },
    LanguageSelector {
        code: "mk-MK",
        ftl: &MK_MK_FTL,
    },
    LanguageSelector {
        code: "ar-SA",
        ftl: &AR_SA_FTL,
    },
    LanguageSelector {
        code: "zh-CN",
        ftl: &ZH_CN_FTL,
    },
    LanguageSelector {
        code: "zh-TW",
        ftl: &ZH_TW_FTL,
    },
    LanguageSelector {
        code: "ja-JP",
        ftl: &JA_JP_FTL,
    },
    LanguageSelector {
        code: "ko-KR",
        ftl: &KO_KR_FTL,
    },
    LanguageSelector {
        code: "hi-IN",
        ftl: &HI_IN_FTL,
    },
    LanguageSelector {
        code: "id-ID",
        ftl: &ID_ID_FTL,
    },
];

pub struct Language {
    bundle: FluentBundle<FluentResource>,
}

impl Language {
    pub fn new(lang: LanguageIdentifier, ftl: String) -> Self {
        let mut bundle = FluentBundle::new_concurrent(vec![lang.clone()]);

        let res = FluentResource::try_new(ftl).expect("Failed to parse FTL");
        bundle.add_resource(res).expect("Failed to add FTL");

        Self { bundle }
    }
}

#[allow(unused)]
pub fn init_i18n() -> Language {
    let langid: LanguageIdentifier = detect_langid();
    let requested_tag = langid.to_string(); // e.g. "pt-BR"

    let ftl = SUPPORTED_LOCALES
        .iter()
        .find(|selector| {
            let found = *selector.code == requested_tag;
            if found {
                info!("Selected exact language {}", selector.code);
            }
            found
        })
        .map(|s| s.ftl)
        .unwrap_or_else(|| {
            let lang = langid.language.as_str();
            SUPPORTED_LOCALES
                .iter()
                .find(|selector| {
                    let found = selector.code.starts_with(lang);
                    if found {
                        info!("Selected approximate language {}", selector.code);
                    }
                    found
                })
                .map(|s| s.ftl)
                .unwrap_or_else(|| {
                    warn!("Falling back to english language. {langid} could not be found");
                    &EN_US_FTL
                })
        });

    Language::new(langid, ftl.parse().expect("Language files to parse"))
}

fn detect_langid() -> LanguageIdentifier {
    // Examples of OS values:
    // "en_US", "de_DE.UTF-8", "fr-FR", "en-US"
    let raw = get_locale().unwrap_or_else(|| {
        warn!("Locale wasn't found");
        "en-US".to_string()
    });

    // Strip encoding and convert underscore to hyphen for BCP-47 / Fluent.
    let cleaned = raw
        .split('.')
        .next()
        .unwrap_or_else(|| {
            warn!("Could not split language {raw}, falling back to english");
            "en-US"
        })
        .replace('_', "-");

    cleaned.parse().unwrap_or_else(|_| {
        warn!("Could not parse language {cleaned}. Falling back to english");
        "en-US".parse().unwrap()
    })
}

pub fn text_resource(lang: &Language, id: &str) -> String {
    text_resource_args(lang, id, &[])
}

pub fn text_resource_args(lang: &Language, id: &str, args: &[(&str, FluentValue<'_>)]) -> String {
    let bundle = &lang.bundle;

    let msg = match bundle.get_message(id) {
        Some(res) => res,
        None => {
            if cfg!(debug_assertions) {
                panic!("Missing message: {}", id);
            } else {
                error!("Missing message: {}", id);
                return format!("<{id}>");
            }
        }
    };

    let pattern = match msg.value() {
        Some(res) => res,
        None => {
            if cfg!(debug_assertions) {
                panic!("Message has no value: {}", id)
            } else {
                error!("Message has no value: {}", id);
                return format!("<{id}>");
            }
        }
    };

    let mut fl_args = FluentArgs::new();
    for (k, v) in args {
        fl_args.set(*k, v.clone());
    }

    let mut errors = vec![];
    let value = bundle.format_pattern(pattern, Some(&fl_args), &mut errors);

    if !errors.is_empty() {
        if cfg!(debug_assertions) {
            panic!("Formatting errors for {}: {:?}", id, errors);
        } else {
            error!("Formatting errors for {}: {:?}", id, errors);
            return format!("<{id}>");
        }
    }

    value.into()
}

#[macro_export]
macro_rules! tr {
    ($lang:expr, $id:expr) => {
        $crate::i18n::text_resource($lang, $id)
    };
    ($lang:expr, $id:expr, $($key:ident = $val:expr),+ $(,)?) => {{
        use fluent::FluentValue;
        let args: &[(&str, FluentValue<'_>)] = &[
            $(
                (stringify!($key), FluentValue::from($val)),
            )+
        ];
        $crate::i18n::text_resource_args($lang, $id, args)
    }};
}

/// Get the appropriate decimal separator for the current locale
pub fn get_decimal_separator() -> char {
    let langid = detect_langid();
    let lang_code = langid.language.as_str();

    match lang_code {
        // Languages that use comma as decimal separator
        "de" | "fr" | "es" | "it" | "pt" | "nl" | "sv" | "da" | "no" | "nb" | "fi" | "pl"
        | "cs" | "sk" | "sl" | "hr" | "sr" | "bs" | "ro" | "bg" | "hu" | "el" | "tr" | "et"
        | "lv" | "lt" | "ru" | "uk" => ',',
        // Languages that use period as decimal separator (default)
        _ => '.',
    }
}

/// Format a float with the appropriate decimal separator for the current locale
pub fn format_decimal(value: f64) -> String {
    let formatted = format!("{:.1}", value);
    if get_decimal_separator() == ',' {
        formatted.replace('.', ",")
    } else {
        formatted
    }
}

/// Format file size in bytes to a human-readable string with appropriate unit
pub fn format_file_size(bytes: u64) -> (String, &'static str) {
    const UNIT: f64 = 1000.0;

    if bytes < UNIT as u64 {
        (bytes.to_string(), "B")
    } else if bytes < (UNIT * UNIT) as u64 {
        (format_decimal(bytes as f64 / UNIT), "KB")
    } else if bytes < (UNIT * UNIT * UNIT) as u64 {
        (format_decimal(bytes as f64 / UNIT / UNIT), "MB")
    } else {
        (format_decimal(bytes as f64 / UNIT / UNIT / UNIT), "GB")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fluent::FluentValue;

    /// Test that all FTL files compile without errors
    #[test]
    fn test_all_ftl_files_compile() {
        // Test all supported locales with their FTL content
        let test_cases = vec![
            ("en-US", &*EN_US_FTL),
            ("de-DE", &*DE_DE_FTL),
            ("pt-PT", &*PT_PT_FTL),
            ("pt-BR", &*PT_BR_FTL),
            ("fr-FR", &*FR_FR_FTL),
            ("es-ES", &*ES_ES_FTL),
            ("it-IT", &*IT_IT_FTL),
            ("nl-NL", &*NL_NL_FTL),
            ("sv-SE", &*SV_SE_FTL),
            ("da-DK", &*DA_DK_FTL),
            ("nb-NO", &*NB_NO_FTL),
            ("fi-FI", &*FI_FI_FTL),
            ("is-IS", &*IS_IS_FTL),
            ("ga-IE", &*GA_IE_FTL),
            ("mt-MT", &*MT_MT_FTL),
            ("ru-RU", &*RU_RU_FTL),
            ("uk-UA", &*UK_UA_FTL),
            ("pl-PL", &*PL_PL_FTL),
            ("cs-CZ", &*CS_CZ_FTL),
            ("sk-SK", &*SK_SK_FTL),
            ("sl-SI", &*SL_SI_FTL),
            ("hr-HR", &*HR_HR_FTL),
            ("sr-RS", &*SR_RS_FTL),
            ("bs-BA", &*BS_BA_FTL),
            ("ro-RO", &*RO_RO_FTL),
            ("bg-BG", &*BG_BG_FTL),
            ("hu-HU", &*HU_HU_FTL),
            ("el-GR", &*EL_GR_FTL),
            ("tr-TR", &*TR_TR_FTL),
            ("et-EE", &*ET_EE_FTL),
            ("lv-LV", &*LV_LV_FTL),
            ("lt-LT", &*LT_LT_FTL),
            ("sq-AL", &*SQ_AL_FTL),
            ("mk-MK", &*MK_MK_FTL),
            ("ar-SA", &*AR_SA_FTL),
            ("zh-CN", &*ZH_CN_FTL),
            ("zh-TW", &*ZH_TW_FTL),
            ("ja-JP", &*JA_JP_FTL),
            ("ko-KR", &*KO_KR_FTL),
            ("hi-IN", &*HI_IN_FTL),
            ("id-ID", &*ID_ID_FTL),
        ];

        for (locale_code, ftl_content) in test_cases {
            // Parse the language identifier
            let langid: LanguageIdentifier = locale_code
                .parse()
                .unwrap_or_else(|e| panic!("Invalid language identifier '{}': {}", locale_code, e));

            // Try to parse the FTL content
            let resource = FluentResource::try_new(ftl_content.to_string()).unwrap_or_else(|e| {
                panic!("Failed to parse FTL content for {}: {:?}", locale_code, e)
            });

            // Create a bundle and add the resource
            let mut bundle = FluentBundle::new_concurrent(vec![langid.clone()]);
            bundle.add_resource(resource).unwrap_or_else(|e| {
                panic!(
                    "Failed to add FTL resource to bundle for {}: {:?}",
                    locale_code, e
                )
            });

            // Verify the bundle was created successfully
            assert!(
                !bundle.locales.is_empty(),
                "Bundle for {} has no locales",
                locale_code
            );
        }
    }

    /// Test the init_i18n function
    #[test]
    fn test_init_i18n() {
        // This will use the system locale or fall back to English
        let language = init_i18n();

        // Try to get a simple message to verify the language object works
        let result = text_resource(&language, "file-received-title");
        assert!(!result.is_empty(), "Translation result should not be empty");
        assert!(
            !result.starts_with('<'),
            "Translation should not be an error placeholder"
        );
    }

    /// Test translation with arguments
    #[test]
    fn test_translation_with_args() {
        let language = init_i18n();

        // Test a translation that takes parameters using the correct FTL parameter names
        let result = text_resource_args(
            &language,
            "file-received-mess-with-contact",
            &[
                ("senderName", FluentValue::from("John")),
                ("fileName", FluentValue::from("test.txt")),
                ("fileSizeValue", FluentValue::from("1.5")),
                ("fileSizeUnit", FluentValue::from("MB")),
                ("requestName", FluentValue::from("Photos")),
            ],
        );

        assert!(
            !result.is_empty(),
            "Translation with args should not be empty"
        );
        assert!(
            !result.starts_with('<'),
            "Translation should not be an error placeholder"
        );
        assert!(
            result.contains("John"),
            "Translation should contain sender name"
        );
        assert!(
            result.contains("test.txt"),
            "Translation should contain file name"
        );
        assert!(
            result.contains("1.5"),
            "Translation should contain file size value"
        );
        assert!(
            result.contains("MB"),
            "Translation should contain file size unit"
        );
        assert!(
            result.contains("Photos"),
            "Translation should contain request name"
        );
    }

    /// Basic test for format_decimal function
    #[test]
    fn test_format_decimal() {
        let result = format_decimal(1234.5);

        // Should contain the number regardless of separator
        assert!(result.contains("1234"), "Should contain the integer part");
        assert!(result.contains("5"), "Should contain the fractional part");

        // Should use either . or , as separator
        assert!(
            result.contains(get_decimal_separator()),
            "Should contain a decimal separator"
        );
    }

    /// Test file size formatting
    #[test]
    fn test_format_file_size() {
        let test_cases = vec![
            (0, ("0", "B")),
            (500, ("500", "B")),
            (1500, ("1", "KB")),
            (1500000, ("1", "MB")),
            (1500000000, ("1", "GB")),
            (15000000000, ("15", "GB")),
        ];

        for (bytes, expected) in test_cases {
            let (value, unit) = format_file_size(bytes);
            assert_eq!(unit, expected.1, "Unit should match for {} bytes", bytes);

            // Value should start with expected number (formatting may add decimals/separators)
            assert!(
                value.starts_with(expected.0),
                "Value '{}' should start with '{}' for {} bytes",
                value,
                expected.0,
                bytes
            );
        }
    }

    /// Test that detect_langid doesn't panic
    #[test]
    fn test_detect_langid() {
        let langid = detect_langid();
        assert!(
            !langid.to_string().is_empty(),
            "Language ID should not be empty"
        );

        // Should be a valid language identifier
        let language_code = langid.language.as_str();
        assert!(
            !language_code.is_empty(),
            "Language code should not be empty"
        );
        assert!(
            language_code.chars().all(|c| c.is_ascii_lowercase()),
            "Language code should be lowercase ASCII"
        );
    }

    /// Test edge case for missing translations (in debug mode this will panic, in release it returns error placeholder)
    #[test]
    fn test_missing_translation() {
        let language = init_i18n();

        // In release mode, should return error placeholder
        if !cfg!(debug_assertions) {
            let result = text_resource(&language, "non-existent-key");
            assert!(
                result.starts_with('<') && result.ends_with('>'),
                "Should return error placeholder for missing key"
            );
        }
    }
}
