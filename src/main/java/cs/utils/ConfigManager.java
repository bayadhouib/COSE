package cs.utils;

import cs.Main;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;



public class ConfigManager {
    private static Properties properties = new Properties();

    static {
        try {
            if (Main.configPath != null) {
                FileInputStream configFile = new FileInputStream(Main.configPath);
                properties.load(configFile);
                configFile.close();
            } else {
                System.out.println("Config Path is not specified in Main Arg");
            }
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }

    public static String getProperty(String property) {
        return properties.getProperty(property);
    }
}
