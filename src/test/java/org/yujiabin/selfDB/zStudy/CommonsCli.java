package org.yujiabin.selfDB.zStudy;

import org.apache.commons.cli.*;
import org.junit.Test;

import java.util.Arrays;

public class CommonsCli {
    @Test
    public void basicParseCLI(String[] args){
        Options options = new Options();

        options.addOption("h", "help", false, "print options' information");
        options.addOption("d", "database", true, "name of a database");
        options.addOption("t", true, "name of a table");

        Option filesOption = OptionBuilder.withArgName("args")
                .withLongOpt("files")
                .hasArgs()
                .withDescription("file names")
                .create("f");
        options.addOption(filesOption);

//  CommandLineParser parser = new DefaultParser();
        CommandLineParser parser = new BasicParser();
        try {
            CommandLine cli = parser.parse(options, args);
            if(cli.hasOption("h")){
                HelpFormatter hf = new HelpFormatter();
                hf.printHelp("Options", options);
            }
            else {
                String database = cli.getOptionValue("d");
                System.out.println("database: " + database);
                String table = cli.getOptionValue("t");
                System.out.println("table: " + table);
                String[] files = cli.getOptionValues("f");
                System.out.println("files: " + Arrays.asList(files));

            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
