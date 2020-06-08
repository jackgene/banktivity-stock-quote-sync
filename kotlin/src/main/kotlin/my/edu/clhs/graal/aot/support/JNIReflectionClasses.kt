package my.edu.clhs.graal.aot.support

import com.oracle.svm.core.annotate.AutomaticFeature
import com.oracle.svm.core.jni.JNIRuntimeAccess
import org.graalvm.nativeimage.hosted.Feature
import org.graalvm.nativeimage.hosted.RuntimeReflection
import org.sqlite.Function
import org.sqlite.Function.Aggregate
import org.sqlite.ProgressHandler
import org.sqlite.core.DB.ProgressObserver
import org.sqlite.core.NativeDB
import java.util.*

@AutomaticFeature
class JNIReflectionClasses : Feature {
    /**
     * Register all constructors and methods on graalvm to reflection support at runtime
     */
    private fun process(clazz: Class<*>) {
        try {
            println("> Declaring class: " + clazz.canonicalName)
            RuntimeReflection.register(clazz)
            for (method in clazz.declaredMethods) {
                println("\t> method: " + method.name + "(" + Arrays.toString(method.parameterTypes) + ")")
                JNIRuntimeAccess.register(method)
                RuntimeReflection.register(method)
            }
            for (field in clazz.declaredFields) {
                println("\t> field: " + field.name)
                JNIRuntimeAccess.register(field)
                RuntimeReflection.register(field)
            }
            for (constructor in clazz.declaredConstructors) {
                println("\t> constructor: " + constructor.name + "(" + constructor.parameterCount + ")")
                JNIRuntimeAccess.register(constructor)
                RuntimeReflection.register(constructor)
            }
        } catch (e: Exception) {
            e.printStackTrace()
        }
    }

    private fun setupClasses() {
        try {
            println("> Loading JNI classes for future reflection support")
            val classes = arrayOf(
                NativeDB::class.java,
                Function::class.java,
                Aggregate::class.java,
                ProgressHandler::class.java,
                Function.Window::class.java,
                ProgressObserver::class.java
            )
            for (clazz in classes) {
                process(clazz)
            }
        } catch (e: Error) {
            if (!e.message!!.contains("The class ImageSingletons can only be used when building native images")) {
                throw e
            }
        }
    }

    override fun beforeAnalysis(access: Feature.BeforeAnalysisAccess) {
        try {
            JNIRuntimeAccess.register(NativeDB::class.java.getDeclaredMethod("_open_utf8", ByteArray::class.java, Int::class.javaPrimitiveType))
        } catch (e: java.lang.Exception) {
            e.printStackTrace()
        }
        setupClasses()
    }
}