package edu.harvard.data.generator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.mapreduce.Mapper;

public class IdentityManagerGenerator {

  private final String hadoopPackage;
  private final String className;
  private final List<String> tableNames;
  private final List<String> mapperNames;
  private final List<String> scrubberNames;

  public IdentityManagerGenerator(final String hadoopPackage, final String className, final List<String> tableNames,
      final List<String> mapperNames, final List<String> scrubberNames) {
    this.hadoopPackage = hadoopPackage;
    this.className = className;
    this.tableNames = tableNames;
    this.mapperNames = mapperNames;
    this.scrubberNames = scrubberNames;
  }

  public void generate(final PrintStream out) {
    out.println("package " + hadoopPackage + ";");
    out.println();
    outputImportStatements(out);
    out.println("public class " + className + " {");
    out.println();
    outputGetTableNames(out);
    out.println();
    outputGetClasses(out, "getMapperClasses", mapperNames);
    out.println();
    outputGetClasses(out, "getScrubberClasses", scrubberNames);
    out.println("}");
  }

  private void outputGetClasses(final PrintStream out, final String mthdName, final List<String> classes) {
    out.println("  @SuppressWarnings(\"rawtypes\")");
    out.println("  public List<Class<? extends Mapper>> " + mthdName + " () {");
    out.println("    final List<Class<? extends Mapper>> classes = new ArrayList<Class<? extends Mapper>>();");
    for (final String cls : classes) {
      out.println("    classes.add(" + cls + ".class);");
    }
    out.println("    return classes;");
    out.println("  }");
  }

  private void outputGetTableNames(final PrintStream out) {
    out.println("  public List<String> getIdentityTableNames() {");
    out.println("    final List<String> names = new ArrayList<String>();");
    for (final String name : tableNames) {
      out.println("    names.add(\"" + name + "\");");
    }
    out.println("    return names;");
    out.println("  }");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + ArrayList.class.getCanonicalName() + ";");
    out.println("import " + List.class.getCanonicalName() + ";");
    out.println("import " + Mapper.class.getCanonicalName() + ";");
    out.println();
    for (final String mapper : mapperNames) {
      out.println("import " + hadoopPackage + "." + mapper + ";");
    }
    out.println();
    for (final String scrubber : scrubberNames) {
      out.println("import " + hadoopPackage + "." + scrubber + ";");
    }
  }

}
