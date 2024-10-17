package cs.utils;

import cs.Main;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


public class Utils {
    
    private static long secondsTotal;
    private static long minutesTotal;
    
    public static void getCurrentTimeStamp() {
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();
        System.out.println(dtf.format(now));
        dtf.format(now);
    }

    public static boolean isValidIRI(String iri) {
        if (iri == null) {
            return false;
        }
        return iri.indexOf(':') > 0;
    }
    
    public static void log(String log) {
        try {
            FileWriter fileWriter = new FileWriter(Constants.RUNTIME_LOGS, true);
            PrintWriter printWriter = new PrintWriter(fileWriter);
            printWriter.println(log);
            printWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    
    public static void logTime(String method, long seconds, long minutes) {
        secondsTotal += seconds;
        minutesTotal += minutes;
        String line = Main.datasetName + "," + method + "," + seconds + "," + minutes + "," + secondsTotal + "," + minutesTotal + "," + Main.extractMaxCardConstraints + "," + Main.datasetPath;
        log(line);
        System.out.println("Time Elapsed " + method + " " + seconds + " sec , " + minutes + " min");
        System.out.println("***** Total Parsing Time " + secondsTotal + " sec , " + minutesTotal + " min *****");
    }
    


}
