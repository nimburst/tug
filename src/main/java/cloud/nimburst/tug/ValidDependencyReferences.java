package cloud.nimburst.tug;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;
import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.ANNOTATION_TYPE;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Target({TYPE, ANNOTATION_TYPE})
@Retention(RUNTIME)
@Constraint(validatedBy = {ValidDependencyReferences.ReferenceValidator.class})
@Documented
public @interface ValidDependencyReferences {

    String message() default "Dependency reference not found";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

    static class ReferenceValidator implements ConstraintValidator<ValidDependencyReferences, TugManifest> {

        @Override
        public void initialize(ValidDependencyReferences constraintAnnotation) {

        }

        @Override
        public boolean isValid(TugManifest manifest, ConstraintValidatorContext context) {

            //TODO
            return true;

        }
    }

}
