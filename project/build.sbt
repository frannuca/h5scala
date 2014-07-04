
//scalaVersion := "2.10.3"

resolvers += "releases"  at "http://oss.sonatype.org/content/repositories/releases"

resolvers += "maven" at "http://repo1.maven.org/maven2"

resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"

resolvers += Resolver.url("sbt-plugin-releases", new URL("http://scalasbt.artifactoryonline.com/scalasbt/sbt-plugin-releases/"))(Resolver.ivyStylePatterns)

resolvers += "Sonatype snapshots" at "http://oss.sonatype.org/content/repositories/snapshots/"

resolvers += "sbt-idea-repo" at "http://mpeltonen.github.com/maven/"


addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")
