package org.openmrs.contrib.glimpse.api.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public class GlimpseUtils {

    public static String readFile(String file) {

        try {
            InputStream in = GlimpseUtils.class.getClassLoader().getResourceAsStream(file);
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            StringBuilder out = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                out.append(line + " ");  // hack, include blank space
            }
            reader.close();

            return out.toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
