package io.github.pretenderdb.cli;

import io.github.pretenderdb.cli.command.ExportCommand;
import io.github.pretenderdb.cli.command.ImportCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Main CLI entry point for PretenderDB export/import tool.
 */
@Command(
    name = "pretender-cli",
    description = "CLI tool for export/import of PretenderDB tables",
    mixinStandardHelpOptions = true,
    version = "1.0.0",
    subcommands = {ExportCommand.class, ImportCommand.class})
public class PretenderCli implements Runnable {

  /**
   * Main entry point.
   *
   * @param args command line arguments
   */
  public static void main(String[] args) {
    final int exitCode = new CommandLine(new PretenderCli()).execute(args);
    System.exit(exitCode);
  }

  @Override
  public void run() {
    // Show help when no subcommand is specified
    CommandLine.usage(this, System.out);
  }
}
