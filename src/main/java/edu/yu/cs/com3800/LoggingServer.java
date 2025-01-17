package edu.yu.cs.com3800;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public interface LoggingServer {

    default Logger initializeLogging(String fileNamePreface) throws IOException {
        return initializeLogging(fileNamePreface,false);
    }
    default Logger initializeLogging(String fileNamePreface, boolean disableParentHandlers) throws IOException {
        if(fileNamePreface == null){
            fileNamePreface = this.getClass().getCanonicalName() + "-" + this.hashCode();
        }
        String loggerName = this.getClass().getCanonicalName() + fileNamePreface;
        return createLogger(loggerName,fileNamePreface,disableParentHandlers);
    }

    static Logger createLogger(String loggerName, String fileNamePreface, boolean disableParentHandlers) throws IOException {
        Logger logger = Logger.getLogger(loggerName);
        //get time suffix
        LocalDateTime date = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-kk_mm");
        String suffix = date.format(formatter);
        String dirName = "logs-" + suffix;
        //setup file handler
        new File(".", dirName + File.separator).mkdirs();
        FileHandler fh = new FileHandler(dirName + File.separator + fileNamePreface + "-Log.txt");
        fh.setFormatter(new SimpleFormatter());
        logger.addHandler(fh);
        logger.setLevel(Level.ALL);
        if(disableParentHandlers){
            logger.setUseParentHandlers(false);
        }
        return logger;
    }
}