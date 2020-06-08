package my.edu.clhs.graal.aot.support

import com.oracle.svm.core.annotate.AutomaticFeature
import org.graalvm.nativeimage.hosted.Feature
import org.graalvm.nativeimage.hosted.Feature.BeforeAnalysisAccess
import org.graalvm.nativeimage.hosted.RuntimeReflection
import java.util.*

@AutomaticFeature
class ReflectionClasses : Feature {
    /**
     * Register all constructors and methods on graalvm to reflection support at runtime
     */
    private fun process(clazz: Class<*>) {
        try {
            println("> Declaring class: " + clazz.canonicalName)
            RuntimeReflection.register(clazz)
            for (method in clazz.declaredMethods) {
                println("\t> method: " + method.name + "(" + Arrays.toString(method.parameterTypes) + ")")
                RuntimeReflection.register(method)
            }
            for (field in clazz.declaredFields) {
                println("\t> field: " + field.name)
                RuntimeReflection.register(field)
            }
            for (constructor in clazz.declaredConstructors) {
                println("\t> constructor: " + constructor.name + "(" + constructor.parameterCount + ")")
                RuntimeReflection.register(constructor)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun setupClasses() {
        println("> Loading regular classes for future reflection support")
        val classes = arrayOf(
            Class.forName("java.util.concurrent.atomic.AtomicLongFieldUpdater\$CASUpdater"),
            Class.forName("kotlin.internal.jdk8.JDK8PlatformImplementations"),
            Class.forName("io.ktor.utils.io.ByteBufferChannel"),
            Class.forName("io.ktor.utils.io.core.BufferFactoryKt"),
            Class.forName("io.ktor.utils.io.internal.ReadWriteBufferState\$IdleEmpty")
        )
        for (clazz in classes) {
            process(clazz)
        }
    }

    override fun beforeAnalysis(access: BeforeAnalysisAccess) {
        setupClasses()
    }
}
