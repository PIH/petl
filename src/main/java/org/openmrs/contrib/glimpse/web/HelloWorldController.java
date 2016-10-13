package org.openmrs.contrib.glimpse.web;

import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
@EnableAutoConfiguration
/**
 * This mainly just serves to demonstrate that we can serve up web content
 * Run the GlimpseApplication class from Intellij, and then navigate to localhost:8080 and see "Hello World"
 */
public class HelloWorldController {

    @RequestMapping("/")
    @ResponseBody
    String home() {
        return "Hello World";
    }
}