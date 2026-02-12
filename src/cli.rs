use clap::{Arg, Command, value_parser};

pub const CMD_SERVE: &str = "serve";
pub const CMD_CONFIG: &str = "config";
pub const CMD_HELP: &str = "help";

pub const ARG_PORT: &str = "port";
pub const ARG_CLIENT: &str = "client";
pub const ARG_COMMAND: &str = "command";
pub const ARG_HOST: &str = "host";
pub const ARG_LOG_LEVEL: &str = "log_level";

pub const DEFAULT_PORT_STR: &str = "8080";
pub const DEFAULT_HOST: &str = "localhost";
pub const DEFAULT_LOG_LEVEL: &str = "info";

pub const CLIENT_CLAUDE: &str = "claude";
pub const CLIENT_QWEN_CODE: &str = "qwen-code";
pub const CLIENT_QWEN_AGENT: &str = "qwen-agent";
const APP_NAME: &str = env!("CARGO_PKG_NAME");
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const APP_ABOUT: &str = "A simple MCP server over HTTP";

pub fn build_cli() -> Command {
    Command::new(APP_NAME)
        .version(APP_VERSION)
        .about(APP_ABOUT)
        .arg_required_else_help(true)
        .subcommand_required(true)
        .disable_help_subcommand(true)
        .subcommand(serve_command())
        .subcommand(config_command())
        .subcommand(help_command())
}

fn serve_command() -> Command {
    Command::new(CMD_SERVE)
        .about("Run the MCP server")
        .arg(
            Arg::new(ARG_HOST)
                .long("host")
                .value_name("HOST")
                .default_value(DEFAULT_HOST)
                .help("Host to bind to (e.g. localhost or 0.0.0.0)"),
        )
        .arg(
            Arg::new(ARG_PORT)
                .short('p')
                .long("port")
                .value_name("PORT")
                .value_parser(value_parser!(u16))
                .default_value(DEFAULT_PORT_STR)
                .help("Port to listen on"),
        )
        .arg(
            Arg::new(ARG_LOG_LEVEL)
                .long("log-level")
                .value_name("LEVEL")
                .default_value(DEFAULT_LOG_LEVEL)
                .help("Logging level (e.g. trace, debug, info, warn, error)"),
        )
}

fn config_command() -> Command {
    Command::new(CMD_CONFIG)
        .about("Print JSON config for MCP clients")
        .arg(
            Arg::new(ARG_CLIENT)
                .long("client")
                .value_name("CLIENT")
                .value_parser([CLIENT_CLAUDE, CLIENT_QWEN_CODE, CLIENT_QWEN_AGENT])
                .default_value(CLIENT_CLAUDE)
                .help("Target client config to render"),
        )
        .arg(
            Arg::new(ARG_HOST)
                .long("host")
                .value_name("HOST")
                .default_value(DEFAULT_HOST)
                .help("Host for the MCP server URL"),
        )
        .arg(
            Arg::new(ARG_PORT)
                .short('p')
                .long("port")
                .value_name("PORT")
                .value_parser(value_parser!(u16))
                .default_value(DEFAULT_PORT_STR)
                .help("Port the local server will listen on"),
        )
}

fn help_command() -> Command {
    Command::new(CMD_HELP)
        .about("Print help for a command")
        .arg(
            Arg::new(ARG_COMMAND)
                .value_name("COMMAND")
                .required(false)
                .value_parser([CMD_SERVE, CMD_CONFIG, CMD_HELP])
                .help("Command to show help for"),
        )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_cli() {
        let cli = build_cli();
        // Test that the CLI builds without errors
        assert_eq!(cli.get_name(), env!("CARGO_PKG_NAME"));
        assert_eq!(cli.get_version().unwrap(), env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_serve_command() {
        let serve_cmd = serve_command();
        assert_eq!(serve_cmd.get_name(), CMD_SERVE);

        // Check that required arguments exist
        let args: Vec<String> = serve_cmd
            .get_arguments()
            .map(|a| a.get_id().to_string())
            .collect();
        assert!(args.contains(&ARG_HOST.to_string()));
        assert!(args.contains(&ARG_PORT.to_string()));
        assert!(args.contains(&ARG_LOG_LEVEL.to_string()));
    }

    #[test]
    fn test_config_command() {
        let config_cmd = config_command();
        assert_eq!(config_cmd.get_name(), CMD_CONFIG);

        // Check that required arguments exist
        let args: Vec<String> = config_cmd
            .get_arguments()
            .map(|a| a.get_id().to_string())
            .collect();
        assert!(args.contains(&ARG_CLIENT.to_string()));
        assert!(args.contains(&ARG_HOST.to_string()));
        assert!(args.contains(&ARG_PORT.to_string()));
    }

    #[test]
    fn test_help_command() {
        let help_cmd = help_command();
        assert_eq!(help_cmd.get_name(), CMD_HELP);

        // Check that required arguments exist
        let args: Vec<String> = help_cmd
            .get_arguments()
            .map(|a| a.get_id().to_string())
            .collect();
        assert!(args.contains(&ARG_COMMAND.to_string()));
    }
}
