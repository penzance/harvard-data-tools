package edu.harvard.data.generator;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.harvard.data.identity.HadoopIdentityKey;
import edu.harvard.data.identity.IdentityScrubber;

public class IdentityManagerGenerator {

  private final String hadoopPackage;
  private final String className;
  private final Map<String, String> mapperNames;
  private final Map<String, String> scrubberNames;

  public IdentityManagerGenerator(final String hadoopPackage, final String className,
      final Map<String, String> mapperNames, final Map<String, String> scrubberNames) {
    this.hadoopPackage = hadoopPackage;
    this.className = className;
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
    outputGetClasses(out, "getMapperClasses", mapperNames,
        "Mapper<Object, Text, ?, HadoopIdentityKey>");
    out.println();
    outputGetClasses(out, "getScrubberClasses", scrubberNames, "IdentityScrubber<?>");
    out.println("}");
  }

  private void outputGetClasses(final PrintStream out, final String mthdName,
      final Map<String, String> classes, final String superType) {
    out.println("  public Map<String, Class<? extends " + superType + ">> " + mthdName + " () {");
    out.println("    final Map<String, Class<? extends " + superType
        + ">> classes = new HashMap<String, Class<? extends " + superType + ">>();");
    for (final String table : classes.keySet()) {
      out.println("    classes.put(\"" + table + "\", " + classes.get(table) + ".class);");
    }
    out.println("    return classes;");
    out.println("  }");
  }

  private void outputGetTableNames(final PrintStream out) {
    out.println("  public List<String> getIdentityTableNames() {");
    out.println("    final List<String> names = new ArrayList<String>();");
    for (final String name : mapperNames.keySet()) {
      out.println("    names.add(\"" + name + "\");");
    }
    out.println("    return names;");
    out.println("  }");
  }

  private void outputImportStatements(final PrintStream out) {
    out.println("import " + ArrayList.class.getCanonicalName() + ";");
    out.println("import " + HashMap.class.getCanonicalName() + ";");
    out.println("import " + List.class.getCanonicalName() + ";");
    out.println("import " + Map.class.getCanonicalName() + ";");
    out.println("import " + Mapper.class.getCanonicalName() + ";");
    out.println("import " + IdentityScrubber.class.getCanonicalName() + ";");
    out.println("import " + HadoopIdentityKey.class.getCanonicalName() + ";");
    out.println("import " + Text.class.getCanonicalName() + ";");
    out.println();
    for (final String mapper : mapperNames.values()) {
      out.println("import " + hadoopPackage + "." + mapper + ";");
    }
    out.println();
    for (final String scrubber : scrubberNames.values()) {
      out.println("import " + hadoopPackage + "." + scrubber + ";");
    }
  }

}
