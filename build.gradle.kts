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

