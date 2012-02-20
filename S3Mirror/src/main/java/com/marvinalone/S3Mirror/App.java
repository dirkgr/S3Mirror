package com.marvinalone.S3Mirror;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.PropertiesCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;

public class App {
    private static final Log s_log = LogFactory.getLog(App.class);
    
    private static final Options s_options;
    static {
        s_options = new Options();
        s_options.addOption(new Option("remove", false, "Remove files in S3"));
        s_options.addOption(new Option("accessKey", true, "S3 access key"));
        s_options.addOption(new Option(
            "secretAccessKey",
            true,
            "Secret S3 access key"));
        s_options.addOption(
            new Option(
                "configFile",
                true,
                String.format(
                    "Configuration file to use. Default is %s",
                    getDefaultConfigFilePath())));
        s_options.addOption(
            "t",
            "threads",
            true,
            "Number of threads to use (default to 2)");
        s_options.addOption(
            "r",
            "repeatInterval",
            true,
            "When specified, the program will keep polling the S3 location as often as specified.");
        s_options.addOption("h", "help", false, "Prints this message");
    }

    public static void main(final String[] args) {
        // configure the AWS SDK to disable certificate checking
        // Unfathomable amounts of lameness in the S3 service make this
        // necessary.
        // See https://forums.aws.amazon.com/thread.jspa?messageID=320332.
        System.setProperty("com.amazonaws.sdk.disableCertChecking", "true");
        
        // parse the command line
        final CommandLineParser parser = new BasicParser();
        final CommandLine commandLine;
        try {
            commandLine = parser.parse(s_options, args);
        } catch(final ParseException e) {
            printHelp(e.getLocalizedMessage());
            System.exit(1);
            return;
        }

        // help?
        if(commandLine.hasOption("h")) {
            printHelp();
            System.exit(0);
            return;
        }

        // authentication
        if(
            (
                commandLine.hasOption("accessKey") &&
                !commandLine.hasOption("secretAccessKey")
            ) || (
                !commandLine.hasOption("accessKey") &&
                commandLine.hasOption("secretAccessKey")
            )
        ) {
            printHelp("accessKey and secretAccessKey must be specified together.");
            System.exit(1);
            return;
        }
        final AWSCredentials creds;
        if(
            commandLine.hasOption("accessKey") &&
            commandLine.hasOption("secretAccessKey")
        ) {
            creds =
                new BasicAWSCredentials(
                    commandLine.getOptionValue("accessKey"),
                    commandLine.getOptionValue("secretAccessKey"));
        } else {
            try {
                creds =
                    new PropertiesCredentials(
                        new File(
                            commandLine.getOptionValue(
                                "configFile",
                                getDefaultConfigFilePath())));
            } catch(final IOException e) {
                printHelp("Could not open configuration file: " + e.getLocalizedMessage());
                System.exit(2);
                return;
            }
        }

        // source and destination
        final String[] otherArgs = commandLine.getArgs();
        if(otherArgs.length != 2) {
            printHelp("Please specify S3 source and local destination.");
            System.exit(1);
        }
        final S3Url s3source;
        try {
            s3source = S3Url.fromString(otherArgs[0]);
        } catch(final IllegalArgumentException e) {
            printHelp("S3 location must be of the form s3://bucket/prefix");
            System.exit(1);
            return;
        }
        final String localDestination = otherArgs[1];
        
        // number of threads
        final int threads;
        final String threadsOption = commandLine.getOptionValue("threads");
        if(threadsOption == null) {
            threads = 2;
        } else {
            try {
                threads = Integer.parseInt(threadsOption);
            } catch(final NumberFormatException e) {
                printHelp("Number of threads must be an integer.");
                System.exit(1);
                return;
            }
        }
        
        // repeat interval
        final double repeatInterval;
        final String repeatIntervalOption =
            commandLine.getOptionValue("repeatInterval");
        if(repeatIntervalOption == null) {
            repeatInterval = -1;
        } else {
            try {
                repeatInterval = Double.parseDouble(repeatIntervalOption) * 60;
            } catch(final NumberFormatException e) {
                printHelp("Repeat interval must be a number.");
                System.exit(1);
                return;
            }
        }
        
        // do the deed
        final AmazonS3 s3 = new AmazonS3Client(creds);
        while(true) {
            final ExecutorService executor = Executors.newFixedThreadPool(threads);
            ObjectListing listing = s3.listObjects(
                s3source.getBucket(),
                s3source.getKey());
            do {
                final Collection<S3ObjectSummary> objectSummaries =
                    listing.getObjectSummaries();
                for(final S3ObjectSummary objectSummary : objectSummaries)
                    executor.execute(
                        new DownloadTask(
                            s3,
                            objectSummary,
                            s3source.getKey(),
                            localDestination,
                            commandLine.hasOption("remove")));
                
                listing =
                    listing.isTruncated() ?
                    s3.listNextBatchOfObjects(listing) :
                    null;
            } while(listing != null);
            
            executor.shutdown();
            try {
                executor.awaitTermination(30, TimeUnit.DAYS);
            } catch(final InterruptedException e) {
                s_log.warn("Waiting for completion was interrupted.", e);
            }
            
            if(repeatInterval < 0)
                break;
            
            try {
                Thread.sleep((long)(repeatInterval * 1000));
            } catch(final InterruptedException e) {
                s_log.warn("Waiting for next execution was interrupted.", e);
            }
        }
    }

    private static void printHelp() {
        printHelp(null);
    }

    private static void printHelp(final String message) {
        final HelpFormatter formatter = new HelpFormatter();
        final PrintWriter writer;
        if(message != null) {
            writer = new PrintWriter(System.err);
            writer.println(message);
            writer.println();
        } else {
            writer = new PrintWriter(System.out);
        }

        formatter.printHelp(
            writer,
            80,
            "S3Mirror [options] <s3 location> <local directory>",
            null,
            s_options,
            0,
            4,
            "If accessKey and secretAccessKey are given, those will "
                + "be used to authenticate. If they are not given, "
                + "authentication information will be read from the file "
                + "specified in the configFile parameter.");
        
        writer.flush();
    }

    private static String getDefaultConfigFilePath() {
        return
            System.getProperty("user.home") +
            File.separator +
            ".s3mirror.cfg";
    }
}
