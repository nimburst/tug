package cloud.nimburst.tug;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import javax.validation.ConstraintViolation;
import javax.validation.ConstraintViolationException;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

public class YamlParser
{
    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

    public static <T> T parseYaml(Path location, Class<T> clazz, boolean validate)
    {
        T value;
        try (InputStream in = Files.newInputStream(location)){
            value = objectMapper.readValue(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read " + clazz.getSimpleName(), e);
        }

        if(validate) {
            ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
            Validator validator = factory.getValidator();
            Set<ConstraintViolation<T>> violations = validator.validate(value);
            if(!violations.isEmpty()) {
                throw new ConstraintViolationException("Invalid " + clazz.getSimpleName(), violations);
            }
        }

        return value;
    }

}
