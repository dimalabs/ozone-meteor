package eu.stratosphere.meteor.client.web;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import eu.stratosphere.meteor.client.common.MeteorContextHandler;

/**
 * This servlet shows JSON script of jobs currently running. It refresh's itself each 5 seconds
 * and actualize OutputServlet after the running job is finished.
 * 
 */
public class RuntimeStateServlet extends AbstractServletGUI {

	/** generated serial version UID **/
	private static final long serialVersionUID = -1757208725461839067L;

	/** job states in JSON **/
	private String jsonStates;

	/** Currently job status for consistency **/
	private boolean isJobRunning = false;

	/**
	 * viewMode represents the viewing mode of this page.
	 * 'process' update the states each 2 seconds
	 * 'view' show the states only, without updates
	 */
	private String viewMode = "process";

	/**
	 * Creates a servlet with js and css for JSON syntax highlighting.
	 * This servlet refresh's itself each 5 seconds.
	 */
	public RuntimeStateServlet() {
		super("RuntimeStates");
		this.addJavaScript("jsonHighlighting.js");
		this.addStylesheet("jsonHighlightBrushes.css");
		this.addStylesheet("meteorFrontend.css");
	}

	/**
	 * If this page is in process mode it refreshs itself each 2 seconds.
	 */
	@Override
	protected void doGet(final HttpServletRequest request, final HttpServletResponse response) throws ServletException,
			IOException {
		// get current states
		this.isJobRunning = MeteorContextHandler.isInProgress();
		this.jsonStates = MeteorContextHandler.getClient().getJobStates();
		this.viewMode = request.getParameter("viewMode");

		// do not reload and close pages in view mode
		if (this.viewMode == null)
			this.viewMode = "process";

		// sets refresh interval in process mode
		if (!this.viewMode.equals("view"))
			response.setHeader("Refresh", "2");

		// back to the roots
		super.doGet(request, response);
	}

	@Override
	protected void writePage(final PrintWriter writer) {
		// hidden parameter
		writer.println("<input type=\"hidden\" name=\"viewmode\" value=\"" + this.viewMode + "\">");

		// create page
		writer.println("<div class=\"main\">");
		writer.println("  <h1>states of jobs</h1>");
		writer.println("  <pre class=\"outputScript\" id=\"script\"></pre>");
		writer.println(" <div class=\"footer\" align=\"right\">Back to start: <a href=\"/\" target=\"_top\">Click here!</a></div>");
		writer.println("</div>");

		// higlighting script
		writer.println("<script type=\"text/javascript\">");
		writer.println("  var obj = [" + this.jsonStates + "];");
		writer.println("  var str = JSON.stringify(obj, null, 4);");
		writer.println("  output( syntaxHighlight(str) );");
		writer.println("</script>");

		// refresh output servlet and close operators
		if (!this.isJobRunning && !this.viewMode.equals("view")) {
			writer.println("<script type=\"text/javascript\">opener.location.reload();</script>");
			writer.println("<script type=\"text/javascript\">window.close();</script>");
		}
	}
}
