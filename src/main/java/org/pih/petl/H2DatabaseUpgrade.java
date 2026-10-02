package org.pih.petl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.boot.context.event.ApplicationEnvironmentPreparedEvent;
import org.springframework.context.ApplicationListener;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * PETL's job history defaults to a file-based H2 database.  H2 2.x can't open a database file written by H2 1.4,
 * which earlier versions of PETL used, so before the datasource is created, such a file is moved aside (to
 * <name>.mv.db.h2-1.4) and a new, empty database is created in its place.  A datasource that isn't an H2 file is left
 * alone.  Registered in META-INF/spring.factories; runs after logging is initialized.
 */
public class H2DatabaseUpgrade implements ApplicationListener<ApplicationEnvironmentPreparedEvent> {

    private static final Log log = LogFactory.getLog(H2DatabaseUpgrade.class);

    public static final String MOVED_SUFFIX = ".h2-1.4";

    private static final String URL_PREFIX = "jdbc:h2:file:";

    // The file header written by H2 1.4 has format:1; H2 2.x writes 2 or higher
    private static final Pattern FORMAT = Pattern.compile("[,:]format:([0-9a-f]+)[,\\s]");

    @Override
    public void onApplicationEvent(ApplicationEnvironmentPreparedEvent event) {
        String url = event.getEnvironment().getProperty("spring.datasource.url");
        File databaseFile = getDatabaseFile(url);
        if (databaseFile != null && isH2Version1File(databaseFile)) {
            File moved = new File(databaseFile.getPath() + MOVED_SUFFIX);
            if (moved.exists()) {
                throw new PetlException("Can't move the H2 1.4 database " + databaseFile + " aside: " + moved + " already exists");
            }
            if (!databaseFile.renameTo(moved)) {
                throw new PetlException("Unable to move the H2 1.4 database " + databaseFile + " to " + moved);
            }
            log.warn("Moved the H2 1.4 database " + databaseFile + ", which this version of H2 can't open, to " + moved +
                    ". Job execution history starts again in a new database. See the README to export the old history.");
        }
    }

    /**
     * @return the .mv.db file of the given H2 file url, or null if the url isn't one
     */
    public static File getDatabaseFile(String url) {
        if (url == null || !url.startsWith(URL_PREFIX)) {
            return null;
        }
        String path = url.substring(URL_PREFIX.length());
        int optionsStart = path.indexOf(';');
        if (optionsStart >= 0) {
            path = path.substring(0, optionsStart);
        }
        return new File(path + ".mv.db");
    }

    /**
     * @return true if the given file exists and its header shows it was written by H2 1.4
     */
    public static boolean isH2Version1File(File file) {
        if (!file.isFile()) {
            return false;
        }
        byte[] header = new byte[256];
        int length;
        try (InputStream in = new FileInputStream(file)) {
            length = in.read(header);
        }
        catch (IOException e) {
            throw new PetlException("Unable to read the H2 database " + file, e);
        }
        if (length <= 0) {
            return false;
        }
        String text = new String(header, 0, length, StandardCharsets.ISO_8859_1);
        if (!text.startsWith("H:")) {
            return false;
        }
        Matcher m = FORMAT.matcher(text);
        return m.find() && Integer.parseInt(m.group(1), 16) < 2;
    }
}
