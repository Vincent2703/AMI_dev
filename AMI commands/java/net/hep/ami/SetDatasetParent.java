package net.hep.ami.command.misc;


import net.hep.ami.QuerySingleton;

import java.util.*;

import javax.sql.RowSet;

import net.hep.ami.*;
import net.hep.ami.role.*;
import net.hep.ami.command.*;
import net.hep.ami.utility.*;
import net.hep.ami.jdbc.Querier;

import org.jetbrains.annotations.*;

@CommandMetadata(role = "AMI_GUEST", visible = true)
public class SetDatasetParent extends AbstractCommand {

    public SetDatasetParent(@NotNull Set<String> userRoles, @NotNull Map<String, String> arguments, long transactionId) {
		super(userRoles, arguments, transactionId);
	}

    @NotNull
	@Override
	public StringBuilder main(@NotNull Map<String, String> arguments) throws Exception {
        QuerySingleton querySingleton = QuerySingleton.INSTANCE;

		String databaseName = arguments.get("database");

        String parentID = arguments.get("parent");
        String childID = arguments.get("child");

		if(Empty.is(databaseName, Empty.STRING_NULL_EMPTY_BLANK)) {
			throw new IllegalArgumentException("<error><![CDATA[The database name is missing.]]></error>");
		}else {
			Querier querier = getQuerier(databaseName);
			querySingleton.setQuerier(querier); // Set the right querier
		}	
		

        if(Empty.is(parentID, Empty.STRING_NULL_EMPTY_BLANK)) {
			throw new IllegalArgumentException("<error><![CDATA[The dataset parent is missing.]]></error>");
		}
		if(Empty.is(childID, Empty.STRING_NULL_EMPTY_BLANK)) {
			throw new IllegalArgumentException("<error><![CDATA[The dataset child is missing.]]></error>");
		}

        querySingleton.setDatasetParent(childID, parentID);

		return new StringBuilder("<info><![CDATA[Dataset parent set with success]]></info>");
    }

    @NotNull
	@Contract(pure = true)
	public static String help()
	{
		return "Set the dataset parent for another dataset.";
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@NotNull
	@Contract(pure = true)
	public static String usage()
	{
		return "-database=\"\" -parent=\"\" -child=\"\"";
	}
}
