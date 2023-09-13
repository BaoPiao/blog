package com.test.Utils;

import com.test.operator.state.broad.TestBroadCast;

import java.net.URISyntaxException;

public class PathUtils {
    public static void main(String[] args) throws URISyntaxException {
        System.out.println(TestBroadCast.class.getPackage().getName().replace(".", "/"));
        System.out.println(PathUtils.class.getClassLoader().getResource("").toURI().getPath().split("target/")[0] + "src/main/resources");
    }

    public static String getCurrentPath(Object c) throws URISyntaxException {

        String projectPath = c.getClass().getResource("").toURI().getPath().split("target/")[0] + "src/main/java/";
        String packagePath = c.getClass().getPackage().getName().replace(".", "/");
        return "file://" + projectPath + packagePath;
    }


}
