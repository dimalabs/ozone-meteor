/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.meteor;

/**
 * A collection of all configuration constants, such as config keys and default values.
 */
public final class MeteorConfigConstants {
	
	// ----------------------------- Web Frontend -----------------------------
	
	/**
	 * The key for Stratosphere's base dir path
	 */
	public static final String STRATOSPHERE_BASE_DIR_PATH_KEY = "stratosphere.base.dir.path";
	
	/**
	 * The key for the config parameter defining port for the pact web-frontend server.
	 */
	public static final String WEB_FRONTEND_PORT_KEY = "meteor.web.port";

	/**
	 * The key for the config parameter defining the directory containing the web documents.
	 */
	public static final String WEB_ROOT_PATH_KEY = "meteor.web.rootpath";
	
	// ------------------------------------------------------------------------
	//                             Default Values
	// ------------------------------------------------------------------------
	
	// ----------------------------- Web Frontend -----------------------------

	/**
	 * The default port to launch the web front end server on.
	 */
	public static final int DEFAULT_WEB_FRONTEND_PORT = 8090;
	
	/**
	 * The default path of the directory containing the web documents.
	 */
	public static final String DEFAULT_WEB_ROOT_DIR = "./resources/web-docs/";
}
