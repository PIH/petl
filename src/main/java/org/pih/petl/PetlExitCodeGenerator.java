package org.pih.petl;

import org.springframework.boot.ExitCodeGenerator;

import java.util.ArrayList;
import java.util.List;

public class PetlExitCodeGenerator implements ExitCodeGenerator {

    private List<Throwable> exceptions = new ArrayList<>();

    @Override
    public int getExitCode() {
        return exceptions.size();
    }

    public List<Throwable> getExceptions() {
        if (exceptions == null) {
            exceptions = new ArrayList<>();
        }
        return exceptions;
    }

    public void setExceptions(List<Throwable> exceptions) {
        this.exceptions = exceptions;
    }

    public void addException(Throwable exception) {
        getExceptions().add(exception);
    }
}
