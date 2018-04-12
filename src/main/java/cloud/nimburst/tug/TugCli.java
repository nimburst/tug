package cloud.nimburst.tug;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.nio.file.Paths;
import java.util.Arrays;

/**
 * The type command line interface for Tug.
 */
public class TugCli
{

    private static final String TOOL = "tug [-push|-pull|-repush] ?[-m <manifest>] ?[-c concurrency] [-a|-r <resources>]";

    private static Options buildOptions() {
        Options options = new Options();
        options.addOption(Option.builder("push")
                .longOpt("push")
                .desc("add resources and dependencies to the cluster")
                .hasArg(false)
                .build());
        options.addOption(Option.builder("pull")
                .longOpt("pull")
                .desc("remove resources and dependent resources from the cluster")
                .hasArg(false)
                .build());
        options.addOption(Option.builder("repush")
                .longOpt("repush")
                .desc("update resources and dependencies in the cluster")
                .hasArg(false)
                .build());
        options.addOption(Option.builder("r")
                .longOpt("resource")
                .desc("comma separated list of resources")
                .hasArgs()
                .build());
        options.addOption(Option.builder("a")
                .longOpt("all")
                .desc("add or remove all resources in the manifest")
                .hasArg(false)
                .build());
        options.addOption(Option.builder("m")
                .longOpt("manifest")
                .desc("the manifest defining the resources, defaults to tug-manifest.yaml in the current directory if omitted")
                .hasArg()
                .build());
        options.addOption(Option.builder("c")
                .longOpt("concurrency")
                .desc("the max number of concurrent resource actions, default to 6 if omitted")
                .hasArg()
                .build());
        options.addOption(Option.builder("help")
                .longOpt("help")
                .desc("print this help message")
                .hasArg(false)
                .build());
        return options;
    }

    private static CommandLine parseOptions(Options options, String[] args) {
        try {
            return new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            return null;
        }
    }

    private static void printHelp(Options options) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp(TOOL, options);
    }

    private static String validate(CommandLine cmd) {

        int actionCount = 0;
        if(cmd.hasOption("push")) {
            actionCount++;
        }
        if(cmd.hasOption("pull")) {
            actionCount++;
        }
        if(cmd.hasOption("repush")) {
            actionCount++;
        }

        if(actionCount > 1) {
            return "only one of -push, -pull, or -repush is allowed";
        }
        if(actionCount == 0) {
            return "one of -push, -pull, or -repush is required";
        }

        int resourceCount = 0;
        if(cmd.hasOption("r")) {
            resourceCount++;
        }
        if(cmd.hasOption("a")) {
            resourceCount++;
        }

        if(resourceCount > 1) {
            return "only one of -a or -r is allowed";
        }
        if(resourceCount == 0) {
            return "one of -a or -r is required";
        }

        String concurrency = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
        if(concurrency != null) {
            int c;
            try {
                c = Integer.parseInt(concurrency);
            } catch (NumberFormatException e) {
                return "concurrency must be an integer";
            }
            if(c <= 0 ) {
                return "concurrency must be greater than zero";
            }
        }

        return "";
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     */
    public static void main(String[] args) {

        Options options = buildOptions();
        CommandLine cmd = parseOptions(options, args);
        if (cmd == null) {
            printHelp(options);
            System.exit(1);
        }

        if (cmd.hasOption("h")) {
            printHelp(options);
        } else {
            String error = validate(cmd);
            if (!error.isEmpty()) {
                System.out.println(error);
                printHelp(options);
                System.exit(1);
            }

            TugAction action = cmd.hasOption("pull") ? TugAction.PULL : cmd.hasOption("push") ? TugAction.PUSH : TugAction.REPUSH;
            String manifest = cmd.hasOption("m") ? cmd.getOptionValue("m") : "tug-manifest.yaml";
            String[] resources = cmd.hasOption("r") ? cmd.getOptionValues("r") : new String[0];
            String concurrency = cmd.hasOption("c") ? cmd.getOptionValue("c") : null;
            int parallelism = 6;
            if(concurrency != null) {
                parallelism = Integer.parseInt(concurrency);
            }

            //TODO valiate manifest exists

            Tug tug = new Tug(parallelism, action, Paths.get(manifest), Arrays.asList(resources));
            tug.execute();
        }
    }
}
