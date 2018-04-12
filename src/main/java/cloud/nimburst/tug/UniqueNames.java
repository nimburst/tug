package cloud.nimburst.tug;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.HashSet;
import java.util.Set;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Unique name validator for the manifest.
 */
@Target({TYPE, ANNOTATION_TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = {UniqueNames.UniqueNamesValidator.class})
@Documented
public @interface UniqueNames {

    String message() default "Deployment names must be unique";
    Class<?>[] groups() default {};
    Class<? extends Payload>[] payload() default {};

    static class UniqueNamesValidator implements ConstraintValidator<UniqueNames, TugManifest> {

        @Override
        public void initialize(UniqueNames constraintAnnotation) {

        }

        @Override
        public boolean isValid(TugManifest manifest, ConstraintValidatorContext context) {

            if (manifest != null && manifest.getDeployments() != null) {
                Set<String> names = new HashSet<>();

                for (String name : names) {
                    if (name != null && !names.add(name)) {
                        context.disableDefaultConstraintViolation();
                        context.buildConstraintViolationWithTemplate("Duplicate deployment name found: " + name)
                                .addConstraintViolation();
                        return false;
                    }
                }
            }
            return true;

        }
    }

}
