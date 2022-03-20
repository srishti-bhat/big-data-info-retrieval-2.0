package uk.ac.gla.dcs.bigdata.apps;

import uk.ac.gla.dcs.bigdata.util.RealizationEngineClient;

public class RemoteSubmit {

	public static void main(String[] args) {

		
		
		System.out.println("#----------------------------------");
		System.out.println("# Big Data AE Remote Deployer");
		System.out.println("#----------------------------------");

		System.out.println("Arguments:");
		System.out.println("  1) TeamID: 3titans");
		System.out.println("  1) Project: big-data-assessed-exercise-final");
		
		
		args = new String[] {
				"3titans", // Change this to your teamid
				"big-data-assessed-exercise-final"    // Change this to your project
			};
		
		if (args.length!=2 || args[0].equalsIgnoreCase("TODO")) {
			System.out.println("TeamID or Project not set, aborting...");
			System.exit(0);
		}
		
		System.out.println("# Stage 1: Register Your GitLab Repo with the Realization Engine");
		System.out.print("Sending Registration Request...");
		boolean registerOk = RealizationEngineClient.registerApplication(args[0], args[1], "bdae");
		if (registerOk) {
			System.out.println("OK");
		} else {
			System.out.println("Failed, Aborting");
			System.exit(1);
		}
		
		System.out.println("# Stage 2: Trigger the Build and Deployment Sequence");
		System.out.print("Sending Application Start Request...");
		boolean startOk = RealizationEngineClient.startBuildOperationSequenceForTeam(args[0]);
		if (startOk) {
			System.out.println("OK");
		} else {
			System.out.println("Failed, Aborting");
			System.exit(1);
		}
	}

	
}
