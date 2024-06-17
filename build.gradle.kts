plugins {
    id("com.diffplug.spotless") version "6.25.0"
}
subprojects {
    apply(plugin = "com.diffplug.spotless")
    configure<com.diffplug.gradle.spotless.SpotlessExtension> {
        format("misc") {
            target("*.gradle", ".gitattributes", ".gitignore")

            trimTrailingWhitespace()
            indentWithTabs()
            endWithNewline()
        }

        /*kotlinGradle {
            ktlint()
            ktfmt()
        }*/

        java {
            removeUnusedImports()
            importOrder()
            googleJavaFormat().reflowLongStrings()
            formatAnnotations()
            toggleOffOn()
        }
    }
}

