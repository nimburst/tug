package cloud.nimburst.tug;

import com.fasterxml.jackson.databind.JsonNode;
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

/**
 * Utilities to parse yaml files.
 */
public class YamlParser {
    private static final ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());


    /**
     * Parse a yaml file into a tree representation.
     *
     * @param location the location of the file
     * @return the configuration tree as a json node object
     */
    public static JsonNode parseYaml(Path location) {
        try (InputStream in = Files.newInputStream(location)) {
            return objectMapper.readTree(in);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read " + location, e);
        }
    }

    /**
     * Converts a tree into a POJO.
     *
     * @param <T>      the POJO class
     * @param node     the tree representation
     * @param clazz    the POJO class
     * @param validate true if bean validation should be performed
     * @return an instance of the POJO class representing the data in the tree
     */
    public static <T> T transformYaml(JsonNode node, Class<T> clazz, boolean validate) {
        T value = objectMapper.convertValue(node, clazz);
        if (validate) {
            validate(value, clazz);
        }
        return value;
    }

    private static <T> void validate(T value, Class<T> clazz) {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<T>> violations = validator.validate(value);
        if (!violations.isEmpty()) {
            throw new ConstraintViolationException("Invalid " + clazz.getSimpleName(), violations);
        }
    }

    /**
     * Parses a yaml file into a POJO
     *
     * @param <T>      the POJO class
     * @param location the location of the yaml file to parse
     * @param clazz    the POJO class
     * @param validate true if bean validation should be performed
     * @return an instance of the POJO class representing the data in the file
     */
    public static <T> T parseYaml(Path location, Class<T> clazz, boolean validate) {

        T value;
        try (InputStream in = Files.newInputStream(location)) {
            value = objectMapper.readValue(in, clazz);
        } catch (IOException e) {
            throw new RuntimeException("Unable to read " + clazz.getSimpleName(), e);
        }

        if (validate) {
            validate(value, clazz);
        }

        return value;
    }

}
