package net.hep.ami.command.misc;


import net.hep.ami.QuerySingleton;

import java.util.*;

import net.hep.ami.*;
import net.hep.ami.role.*;
import net.hep.ami.command.*;
import net.hep.ami.utility.*;

import org.jetbrains.annotations.*;

import net.hep.ami.QuerySingleton;

@CommandMetadata(role = "AMI_GUEST", visible = true)
public class SetDatasetParent extends AbstractCommand {

    public SetDatasetParent(@NotNull Set<String> userRoles, @NotNull Map<String, String> arguments, long transactionId) {
		super(userRoles, arguments, transactionId);
	}

    @NotNull
	@Override
	public StringBuilder main(@NotNull Map<String, String> arguments) throws Exception {
        QuerySingleton querySingleton = QuerySingleton.INSTANCE;

        String parentID = arguments.get("parent");
        String childID = arguments.get("child");

        if(Empty.is(parentID, Empty.STRING_NULL_EMPTY_BLANK)) {
			throw new IllegalArgumentException("The dataset parent is missing.");
		}
		if(Empty.is(childID, Empty.STRING_NULL_EMPTY_BLANK)) {
			throw new IllegalArgumentException("The dataset child is missing.");
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
		return "-parent=\"\" child=\"\"";
	}
}
