/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.sopremo.query;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import eu.stratosphere.sopremo.io.CsvFormat;
import eu.stratosphere.sopremo.io.JsonFormat;
import eu.stratosphere.sopremo.io.Sink;
import eu.stratosphere.sopremo.io.SopremoFormat;
import eu.stratosphere.sopremo.io.Source;
import eu.stratosphere.sopremo.operator.Operator;
import eu.stratosphere.sopremo.packages.IConstantRegistry;
import eu.stratosphere.sopremo.packages.IFunctionRegistry;
import eu.stratosphere.sopremo.packages.NameChooserProvider;

/**
 */
public class PackageManager implements ParsingScope {
	private final Map<String, PackageInfo> packages = new HashMap<String, PackageInfo>();

	private final List<File> jarPathLocations = new ArrayList<File>(Arrays.asList(new File(".")));

	private final StackedConstantRegistry constantRegistries;

	private final StackedFunctionRegistry functionRegistries;

	private final StackedTypeRegistry typeRegistries;

	private final StackedConfObjectRegistry<Operator<?>> operatorRegistries;

	private final StackedConfObjectRegistry<SopremoFormat> fileFormatRegistries;

	private final NameChooserProvider nameChooserProvider;


	public PackageManager(final NameChooserProvider nameChooserProvider) {
		this.nameChooserProvider = nameChooserProvider;

		this.constantRegistries = new StackedConstantRegistry(nameChooserProvider.getConstantNameChooser());
		this.functionRegistries = new StackedFunctionRegistry(nameChooserProvider.getFunctionNameChooser());
		this.typeRegistries = new StackedTypeRegistry(nameChooserProvider.getTypeNameChooser());
		this.operatorRegistries = new StackedConfObjectRegistry<Operator<?>>(
			nameChooserProvider.getOperatorNameChooser(), nameChooserProvider.getPropertyNameChooser());
		this.fileFormatRegistries = new StackedConfObjectRegistry<SopremoFormat>(
			nameChooserProvider.getFormatNameChooser(), nameChooserProvider.getPropertyNameChooser());

		final AdditionalInfoResolver operatorOrFormatResolver =
			new AdditionalInfoResolver.OperatorOrFormat(this.operatorRegistries, this.fileFormatRegistries);
		final IConfObjectRegistry<Operator<?>> ioRegistry = new DefaultConfObjectRegistry<Operator<?>>(
			nameChooserProvider.getOperatorNameChooser(), nameChooserProvider.getPropertyNameChooser());
		ioRegistry.put(Sink.class, operatorOrFormatResolver);
		ioRegistry.put(Source.class, operatorOrFormatResolver);
		this.operatorRegistries.addLast(ioRegistry);

		final AdditionalInfoResolver formatResolver = new AdditionalInfoResolver.Format(this.fileFormatRegistries);
		final IConfObjectRegistry<SopremoFormat> defaultFormatRegistry = new DefaultConfObjectRegistry<SopremoFormat>(
			nameChooserProvider.getFormatNameChooser(), nameChooserProvider.getPropertyNameChooser());
		defaultFormatRegistry.put(CsvFormat.class, formatResolver);
		defaultFormatRegistry.put(JsonFormat.class, formatResolver);
		this.fileFormatRegistries.addLast(defaultFormatRegistry);
	}

	public void addAll(final PackageManager packageManager) {
		this.constantRegistries.push(packageManager.getConstantRegistry());
		this.functionRegistries.push(packageManager.getFunctionRegistry());
		this.operatorRegistries.push(packageManager.getOperatorRegistry());
		this.fileFormatRegistries.push(packageManager.getFileFormatRegistry());
		this.typeRegistries.push(packageManager.getTypeRegistry());
		this.packages.putAll(packageManager.packages);
		this.jarPathLocations.addAll(packageManager.getJarPathLocations());
	}

	/**
	 * Adds a location where the package loader should look for Sopremo Jars.
	 * 
	 * @param jarPathLocation
	 *        the defaultJarPath to set
	 */
	public void addJarPathLocation(final File jarPathLocation) {
		if (jarPathLocation == null)
			throw new NullPointerException("jarPathLocation must not be null");

		this.jarPathLocations.add(jarPathLocation);
	}

	@Override
	public IConstantRegistry getConstantRegistry() {
		return this.constantRegistries;
	}

	/**
	 * Returns the fileFormatRegistries.
	 * 
	 * @return the fileFormatRegistries
	 */
	@Override
	public IConfObjectRegistry<SopremoFormat> getFileFormatRegistry() {
		return this.fileFormatRegistries;
	}

	@Override
	public IFunctionRegistry getFunctionRegistry() {
		return this.functionRegistries;
	}

	/**
	 * Returns the names of the imported packages.
	 * 
	 * @return the packages
	 */
	public Collection<PackageInfo> getImportedPackages() {
		return this.packages.values();
	}

	/**
	 * Returns the jarPathLocations.
	 * 
	 * @return the jarPathLocations
	 */
	public List<File> getJarPathLocations() {
		return this.jarPathLocations;
	}

	/**
	 * Returns the operatorFactory.
	 * 
	 * @return the operatorFactory
	 */
	@Override
	public IConfObjectRegistry<Operator<?>> getOperatorRegistry() {
		return this.operatorRegistries;
	}

	/**
	 * Imports sopremo-&lt;packageName&gt;.jar or returns a cached package
	 * structure.
	 * 
	 * @param packageName
	 */
	public PackageInfo getPackageInfo(final String packageName) {
		PackageInfo packageInfo = this.packages.get(packageName);
		if (packageInfo == null) {
			try {
				List<File> packagePath = this.findPackageInClassPath(packageName);
				if (!packagePath.isEmpty()) {
					packageInfo =
						new PackageInfo(packageName, ClassLoader.getSystemClassLoader(), this.nameChooserProvider);
					File jarFile = null;
					for (final File file : packagePath)
						if (file.isFile() && file.getName().endsWith(".jar")) {
							jarFile = file;
							break;
						}
					if (jarFile != null) {
						QueryUtil.LOG.debug("adding package from classpath: " + packagePath);
						packageInfo.importFrom(jarFile, packageName);
					} else packageInfo.importFromProject(packagePath.get(0));
				} else {
					final File jarLocation = this.findPackageInJarPathLocations(packageName);
					if (jarLocation == null)
						throw new IllegalArgumentException(String.format("no package %s found", packageName));
					packageInfo = new PackageInfo(packageName, this.nameChooserProvider);
					QueryUtil.LOG.debug("adding package from jar path: " + packageName);
					packageInfo.importFrom(jarLocation, packageName);
				}
			} catch (final Exception e) {
				throw new IllegalArgumentException(String.format(
					"could not load package; please make sure that sopremo-%s.jar is in classpath", packageName), e);
			}
			this.packages.put(packageName, packageInfo);
		}
		return packageInfo;
	}

	/**
	 * Returns the typeRegistries.
	 * 
	 * @return the typeRegistries
	 */
	@Override
	public StackedTypeRegistry getTypeRegistry() {
		return this.typeRegistries;
	}

	public void importPackage(final PackageInfo packageInfo) {
		this.constantRegistries.push(packageInfo.getConstantRegistry());
		this.functionRegistries.push(packageInfo.getFunctionRegistry());
		this.operatorRegistries.push(packageInfo.getOperatorRegistry());
		this.fileFormatRegistries.push(packageInfo.getFileFormatRegistry());
		this.typeRegistries.push(packageInfo.getTypeRegistry());
	}

	public void importPackage(final String packageName) {
		this.importPackage(this.getPackageInfo(packageName));
	}

	public void importPackageFrom(final String packageName, final File jarFile) {
		try {
			final PackageInfo packageInfo = new PackageInfo(packageName, this.nameChooserProvider);
			packageInfo.importFrom(jarFile, packageName);
			this.packages.put(packageName, packageInfo);
			this.importPackage(packageInfo);
		} catch (final Exception e) {
			throw new IllegalArgumentException("Cannot load package from directory", e);
		}
	}

	/*
	 * (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("Package manager with packages %s", this.packages);
	}

	protected List<File> findPackageInClassPath(final String packageName) {
		final String sopremoPackage = getJarFileNameForPackageName(packageName);
		// check in class paths
		final List<File> paths = new ArrayList<File>();
		for (final String path : this.getClasspath().split(File.pathSeparator)) {
			final int pathIndex = path.indexOf(sopremoPackage);
			if (pathIndex == -1)
				continue;
			// preceding character must be a file separator
			if (pathIndex > 0 && path.charAt(pathIndex - 1) != File.separatorChar)
				continue;
			final int nextIndex = pathIndex + sopremoPackage.length();
			// next character must be '.', '-', or file separator
			if (nextIndex < path.length() && path.charAt(nextIndex) != File.separatorChar &&
				path.charAt(nextIndex) != '.' && path.charAt(nextIndex) != '-')
				continue;
			paths.add(new File(path));
		}

		return paths;
	}

	protected File findPackageInJarPathLocations(final String packageName) {
		final String sopremoPackage = getJarFileNameForPackageName(packageName);
		// look in additional directories
		final Pattern filePattern = Pattern.compile(sopremoPackage + "(-[0-9].*)?\\.jar");

		for (final File jarPathLocation : this.jarPathLocations) {
			final File[] jars = jarPathLocation.listFiles(new FilenameFilter() {
				@Override
				public boolean accept(final File dir, final String name) {
					return filePattern.matcher(name).matches();
				}
			});
			if (jars.length > 0)
				return jars[0];
		}
		return null;
	}

	private String getClasspath() {
		// FIXME hack for running integration tests wit maven
		String classpath = System.getProperty("surefire.test.class.path");
		if (classpath == null || classpath.equals(""))
			classpath = System.getProperty("java.class.path");
		return classpath;
	}

	protected static String getJarFileNameForPackageName(final String packageName) {
		return "sopremo-" + packageName;
	}
}
