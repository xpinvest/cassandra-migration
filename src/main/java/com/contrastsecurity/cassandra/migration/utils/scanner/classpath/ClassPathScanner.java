/**
 * Copyright 2010-2015 Axel Fontaine
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.contrastsecurity.cassandra.migration.utils.scanner.classpath;

import com.contrastsecurity.cassandra.migration.CassandraMigrationException;
import com.contrastsecurity.cassandra.migration.logging.Log;
import com.contrastsecurity.cassandra.migration.logging.LogFactory;
import com.contrastsecurity.cassandra.migration.utils.ClassUtils;
import com.contrastsecurity.cassandra.migration.utils.FeatureDetector;
import com.contrastsecurity.cassandra.migration.utils.UrlUtils;
import com.contrastsecurity.cassandra.migration.utils.scanner.Resource;

import java.io.IOException;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;

public class ClassPathScanner {
    private static final Log LOG = LogFactory.getLog(ClassPathScanner.class);

    /**
     * The ClassLoader for loading migrations on the classpath.
     */
    private final ClassLoader classLoader;

    /**
     * Creates a new Classpath scanner.
     *
     * @param classLoader The ClassLoader for loading migrations on the classpath.
     */
    public ClassPathScanner(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    /**
     * Scans the classpath for resources under the specified location, starting with the specified prefix and ending with
     * the specified suffix.
     *
     * @param path   The path in the classpath to start searching. Subdirectories are also searched.
     * @param prefix The prefix of the resource names to match.
     * @param suffix The suffix of the resource names to match.
     * @return The resources that were found.
     * @throws IOException when the location could not be scanned.
     */
    public Resource[] scanForResources(String path, String prefix, String suffix) throws IOException {
        LOG.debug("Scanning for classpath resources at '" + path + "' (Prefix: '" + prefix + "', Suffix: '" + suffix + "')");

        Set<Resource> resources = new TreeSet<Resource>();

        Set<String> resourceNames = findResourceNames(path, prefix, suffix);
        for (String resourceName : resourceNames) {
            resources.add(new ClassPathResource(resourceName, classLoader));
            LOG.debug("Found resource: " + resourceName);
        }

        return resources.toArray(new Resource[resources.size()]);
    }

    /**
     * Scans the classpath for concrete classes under the specified package implementing this interface.
     * Non-instantiable abstract classes are filtered out.
     *
     * @param location             The location (package) in the classpath to start scanning.
     *                             Subpackages are also scanned.
     * @param implementedInterface The interface the matching classes should implement.
     * @return The non-abstract classes that were found.
     * @throws Exception when the location could not be scanned.
     */
    public Class<?>[] scanForClasses(String location, Class<?> implementedInterface) throws Exception {
        LOG.debug("Scanning for classes at '" + location + "' (Implementing: '" + implementedInterface.getName() + "')");

        List<Class<?>> classes = new ArrayList<Class<?>>();

        Set<String> resourceNames = findResourceNames(location, "", ".class");
        for (String resourceName : resourceNames) {
            String className = toClassName(resourceName);
            Class<?> clazz = classLoader.loadClass(className);

            if (Modifier.isAbstract(clazz.getModifiers()) || clazz.isEnum() || clazz.isAnonymousClass()) {
                LOG.debug("Skipping non-instantiable class: " + className);
                continue;
            }

            if (!implementedInterface.isAssignableFrom(clazz)) {
                continue;
            }

            try {
                ClassUtils.instantiate(className, classLoader);
            } catch (Exception e) {
                throw new CassandraMigrationException("Unable to instantiate class: " + className, e);
            }

            classes.add(clazz);
            LOG.debug("Found class: " + className);
        }

        return classes.toArray(new Class<?>[classes.size()]);
    }

    /**
     * Converts this resource name to a fully qualified class name.
     *
     * @param resourceName The resource name.
     * @return The class name.
     */
    private String toClassName(String resourceName) {
        String nameWithDots = resourceName.replace("/", ".");
        return nameWithDots.substring(0, (nameWithDots.length() - ".class".length()));
    }

    /**
     * Finds the resources names present at this location and below on the classpath starting with this prefix and
     * ending with this suffix.
     *
     * @param path   The path on the classpath to scan.
     * @param prefix The filename prefix to match.
     * @param suffix The filename suffix to match.
     * @return The resource names.
     * @throws IOException when scanning this location failed.
     */
    private Set<String> findResourceNames(String path, String prefix, String suffix) throws IOException {
        Set<String> resourceNames = new TreeSet<String>();

        List<URL> locationsUrls = getLocationUrlsForPath(path);
        for (URL locationUrl : locationsUrls) {
            LOG.debug("Scanning URL: " + locationUrl.toExternalForm());

            UrlResolver urlResolver = createUrlResolver(locationUrl.getProtocol());
            URL resolvedUrl = urlResolver.toStandardJavaUrl(locationUrl);

            String protocol = resolvedUrl.getProtocol();
            ClassPathLocationScanner classPathLocationScanner = createLocationScanner(protocol);
            if (classPathLocationScanner == null) {
                String scanRoot = UrlUtils.toFilePath(resolvedUrl);
                LOG.warn("Unable to scan location: " + scanRoot + " (unsupported protocol: " + protocol + ")");
            } else {
                resourceNames.addAll(classPathLocationScanner.findResourceNames(path, resolvedUrl));
            }
        }

        return filterResourceNames(resourceNames, prefix, suffix);
    }

    /**
     * Gets the physical location urls for this logical path on the classpath.
     *
     * @param path The path on the classpath.
     * @return The underlying physical URLs.
     * @throws IOException when the lookup fails.
     */
    private List<URL> getLocationUrlsForPath(String path) throws IOException {
        List<URL> locationUrls = new ArrayList<URL>();

        if (classLoader.getClass().getName().startsWith("com.ibm")) {
            // WebSphere
            Enumeration<URL> urls = classLoader.getResources(path + "/flyway.location");
            if (!urls.hasMoreElements()) {
                throw new CassandraMigrationException("Unable to determine URL for classpath location: " + path + " (ClassLoader: " + classLoader + ")"
                        + " On WebSphere an empty file named flyway.location must be present on the classpath location for WebSphere to find it!");
            }
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                locationUrls.add(new URL(URLDecoder.decode(url.toExternalForm(), "UTF-8").replace("/flyway.location", "")));
            }
        } else {
            Enumeration<URL> urls = classLoader.getResources(path);
            if (!urls.hasMoreElements()) {
                throw new CassandraMigrationException("Unable to determine URL for classpath location: " + path + " (ClassLoader: " + classLoader + ")");
            }

            while (urls.hasMoreElements()) {
                locationUrls.add(urls.nextElement());
            }
        }

        return locationUrls;
    }

    /**
     * Creates an appropriate URL resolver scanner for this url protocol.
     *
     * @param protocol The protocol of the location url to scan.
     * @return The url resolver for this protocol.
     */
    private UrlResolver createUrlResolver(String protocol) {
        return new DefaultUrlResolver();
    }

    /**
     * Creates an appropriate location scanner for this url protocol.
     *
     * @param protocol The protocol of the location url to scan.
     * @return The location scanner or {@code null} if it could not be created.
     */
    private ClassPathLocationScanner createLocationScanner(String protocol) {
        if ("file".equals(protocol)) {
            return new FileSystemClassPathLocationScanner();
        }

        if ("jar".equals(protocol)
                || "zip".equals(protocol) //WebLogic
                || "wsjar".equals(protocol) //WebSphere
                ) {
            return new JarFileClassPathLocationScanner();
        }

        FeatureDetector featureDetector = new FeatureDetector(classLoader);

        return null;
    }

    /**
     * Filters this list of resource names to only include the ones whose filename matches this prefix and this suffix.
     *
     * @param resourceNames The names to filter.
     * @param prefix        The prefix to match.
     * @param suffix        The suffix to match.
     * @return The filtered names set.
     */
    private Set<String> filterResourceNames(Set<String> resourceNames, String prefix, String suffix) {
        Set<String> filteredResourceNames = new TreeSet<String>();
        for (String resourceName : resourceNames) {
            String fileName = resourceName.substring(resourceName.lastIndexOf("/") + 1);
            if (fileName.startsWith(prefix) 
            &&  fileName.endsWith(suffix)
            && (fileName.length() > (prefix + suffix).length())) {
                filteredResourceNames.add(resourceName);
            } else {
                LOG.debug("Filtering out resource: " + resourceName + " (filename: " + fileName + ")");
            }
        }
        return filteredResourceNames;
    }
}
