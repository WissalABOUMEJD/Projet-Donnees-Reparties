package config;

public class Project {
	public static String PATH = "/home/hagimont/hagidoop/";
	public static String listMachines[] = {"localhost", "localhost", "localhost"};
	public static int listServeur[] = {4000, 4001, 4002};
	public enum Cmd {READ , WRITE , DELETE };
	public static int fragmentsize = 128000;
	public static int nbrFragments = 128000;
}
