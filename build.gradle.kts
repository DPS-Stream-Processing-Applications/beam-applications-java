plugins {
    id("com.diffplug.spotless") version "6.25.0"
}
subprojects {
    apply(plugin = "com.diffplug.spotless")
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        format("misc", {
                // define the files to apply `misc` to
                target("*.gradle", ".gitattributes", ".gitignore")

                // define the steps to apply to those files
                trimTrailingWhitespace()
                indentWithTabs() // or spaces. Takes an integer argument if you don't like 4
                endWithNewline()
               })

        kotlinGradle {
            ktlint()
        }

        java {
            removeUnusedImports()
                importOrder()
                googleJavaFormat("1.8").aosp().reflowLongStrings()
                formatAnnotations()
        }
    }
}

