package net.hep.ami.rest;

import java.text.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.security.cert.*;

import jakarta.ws.rs.*;
import jakarta.ws.rs.core.*;
import jakarta.servlet.http.*;

import net.hep.ami.*;
import net.hep.ami.data.*;
import net.hep.ami.jdbc.*;

import org.jetbrains.annotations.*;

/* Custom imports */
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.JwtParser;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

@Path("/token")
public class Token
{	
	/*----------------------------------------------------------------------------------------------------------------*/
	@POST
	@Path("jwt")
	@Consumes(MediaType.WILDCARD)
	@Produces(MediaType.TEXT_PLAIN)
	public Response getByJWT(
		@Context HttpServletRequest request,
		@HeaderParam("Authorization") String authorizationHeader
	) {

		if(authorizationHeader == null || !authorizationHeader.startsWith("Bearer ")) {
			return Response.status(Response.Status.UNAUTHORIZED)
					.entity("Missing or invalid Authorization header")
					.build();
					
		}else {
			String JWT = authorizationHeader.substring(7);
			return getFromJWT(request, JWT);
		}
	}


	/*----------------------------------------------------------------------------------------------------------------*/

	@GET
	@Path("password")
	@Produces(MediaType.TEXT_PLAIN)
	public Response getByPassword(
		@Context HttpServletRequest request,
		@QueryParam("username") @DefaultValue("") String username,
		@QueryParam("password") @DefaultValue("") String password
	 ) {
		return get(request, username, password, null, null, null, null);
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@GET
	@Path("certificate")
	@Produces(MediaType.TEXT_PLAIN)
	public Response getByCertificate(
		@NotNull @Context HttpServletRequest request
	 ) {
		/*------------------------------------------------------------------------------------------------------------*/

		X509Certificate[] certificates = (X509Certificate[]) request.getAttribute("jakarta.servlet.request.X509Certificate");

		if(certificates != null)
		{
			for(X509Certificate certificate: certificates)
			{
				if(!SecuritySingleton.isProxy(certificate))
				{
					return get(
						request,
						null,
						null,
						SecuritySingleton.getDN(certificate.getSubjectX500Principal()),
						SecuritySingleton.getDN(certificate.getIssuerX500Principal()),
						new SimpleDateFormat("EEE, d MMM yyyy", Locale.US).format(
							certificate.getNotBefore()
						),
						new SimpleDateFormat("EEE, d MMM yyyy", Locale.US).format(
							certificate.getNotAfter()
						)
					);
				}
			}
		}

		/*------------------------------------------------------------------------------------------------------------*/

		return Response.status(Response.Status.UNAUTHORIZED).build();

		/*------------------------------------------------------------------------------------------------------------*/
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	@DELETE
	public Response delete(
		@NotNull @Context HttpServletRequest request
	 ) {
		request.getSession(true).removeAttribute("token");

		return Response.status(Response.Status./*-*/OK/*-*/).build();
	}

	/*----------------------------------------------------------------------------------------------------------------*/

	private Response getFromJWT(HttpServletRequest request, String JWT) {
		if(JWT == null || JWT.isBlank()) {
			System.out.println("JWT is missing");
			return Response.status(Response.Status.BAD_REQUEST).build();
		}

		String JWTPublicKeyPath = "/JWTPublicKey.pub";

		// 1) Get the the public key
        PublicKey publicKey = null; 
        try {
            InputStream inputStream = Token.class.getResourceAsStream(JWTPublicKeyPath);
            if(inputStream != null) {
                String JWTPublicKey = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

                // Make a PublicKey object to decode the JWT
                byte[] byteKey = Base64.getMimeDecoder().decode(JWTPublicKey);
                X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(byteKey);
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");

                publicKey = keyFactory.generatePublic(X509publicKey);
            }else{
				System.out.println("JWT Public Key not found!");
				return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
            }
        }
        catch(Exception error) {
			System.out.println(error);
            return Response.status(Response.Status.UNAUTHORIZED).build();
        }

        // 2) Decode the JWT and get the payload
        JwtParser jwtParser = Jwts.parser()
            .verifyWith(publicKey)
            .build();
        try {
            jwtParser.parseSignedClaims(JWT);

            Base64.Decoder decoder = Base64.getUrlDecoder();
            String[] chunks = JWT.split("\\.");
            String payload = new String(decoder.decode(chunks[1]));

			// Extract the data from the JWT
			Pattern patternSub = Pattern.compile("\"sub\":\\s*\"([^\\\"]+)\"");
			Pattern patternIssIsGalaxy = Pattern.compile("\"iss\":\\s*\"Galaxy_export\"");
			Pattern patternAudIsAMI = Pattern.compile("\"aud\":\\s*\"AMI_import\"");
			
			Boolean issIsGalaxy = patternIssIsGalaxy.matcher(payload).find();
			Boolean audIsAMI = patternAudIsAMI.matcher(payload).find();

			// Do some checking...
			if(!issIsGalaxy || !audIsAMI) {
				System.out.println("Bad issuer and/or audience");
				return Response.status(Response.Status.FORBIDDEN).build();
			}

			String sub = null; // User email
			Matcher matcherSub = patternSub.matcher(payload);
			if(matcherSub.find()) {
				sub = matcherSub.group(1);
			}else {
				System.out.println("No subscriber found");
				return Response.status(Response.Status.BAD_REQUEST).build();
			}

			String userID = null;
			RouterQuerier querier = new RouterQuerier();
			try {
				Row rowUser = querier.executeSQLQuery("router_user", String.format(
					"SELECT user.id\n" + 
					"FROM router_user user\n" + 
					"INNER JOIN router_user_role user_role ON user_role.userFK = user.id\n" + 
					"INNER JOIN router_role role ON role.id = user_role.roleFK\n" + 
					"WHERE user.email = '%s' AND role.role IN ('AMI_ADMIN', 'AMI_SUDOER', 'AMI_USER') LIMIT 1;", 
					sub
				)).getFirst();
				userID = rowUser.getValue(0);
			}catch(Exception error) {
				System.out.println(String.format("The user doesn't exist or has sufficient permission (%s)", error));
				return Response.status(Response.Status.FORBIDDEN).build();
			}


			/*-----------------------------------------------------------------*/
			/* BUILD TOKEN                                                     */
			/*-----------------------------------------------------------------*/

			HttpSession session = request.getSession(true);

			session.setMaxInactiveInterval(120); // 2mn token validity

			/*-----------------------------------------------------------------*/

			Map<String, String> token = new HashMap<>();

			token.put("AMIUser", userID);
			token.put("clientDN", "");
			token.put("issuerDN", "");
			token.put("notBefore", "");
			token.put("notAfter", "");

			/*-----------------------------------------------------------------*/

			session.setAttribute("token", token);

			/*------------------------------------------------------------------------------------------------------------*/

			String JSessionID = session.getId();
			return Response.ok(JSessionID).build();

        }catch(Exception error) {
			System.out.println(error);
			return Response.status(Response.Status.UNAUTHORIZED).build(); // Could not verify JWT token integrity
        }
	}


	/*----------------------------------------------------------------------------------------------------------------*/

	private Response get(HttpServletRequest request, @Nullable String AMIUser, @Nullable String AMIPass, @Nullable String clientDN, @Nullable String issuerDN, String notBefore, String notAfter)
	{
		/*------------------------------------------------------------------------------------------------------------*/
		/* CHECK CREDENTIALS                                                                                          */
		/*------------------------------------------------------------------------------------------------------------*/

		try
		{
			RouterQuerier querier = new RouterQuerier();

			try
			{
				List<Row> rows;

				boolean checkPasswork;

				/*----------------------------------------------------------------------------------------------------*/

				/**/ if(clientDN != null
				        &&
				        issuerDN != null
				 ) {
					rows = querier.executeSQLQuery("router_user", "SELECT `AMIUser`, `AMIPass` FROM `router_user` WHERE `clientDN` = ?#0 AND `issuerDN` = ?#1", clientDN, issuerDN).getAll();

					checkPasswork = false;
				}
				else if(AMIUser != null
				        &&
				        AMIPass != null
				 ) {
					rows = querier.executeSQLQuery("router_user", "SELECT `AMIUser`, `AMIPass` FROM `router_user` WHERE `AMIUser` = ?0", AMIUser).getAll();

					checkPasswork = true;
				}
				else
				{
					return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
				}

				/*----------------------------------------------------------------------------------------------------*/

				if(rows.size() != 1)
				{
					return Response.status(Response.Status.UNAUTHORIZED).build();
				}

				AMIUser = rows.get(0).getValue(0);

				/*----------------------------------------------------------------------------------------------------*/

				if(checkPasswork)
				{
					try
					{
						String hashed = rows.get(0).getValue(1);

						SecuritySingleton.checkPassword(AMIUser, AMIPass, hashed);
					}
					catch(Exception e)
					{
						return Response.status(Response.Status.UNAUTHORIZED).build();
					}
				}

				/*----------------------------------------------------------------------------------------------------*/
			}
			finally
			{
				querier.rollbackAndRelease();
			}
		}
		catch(Exception e)
		{
			return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
		}

		/*-----------------------------------------------------------------*/
		/* BUILD TOKEN                                                     */
		/*-----------------------------------------------------------------*/

		HttpSession session = request.getSession(true);

		session.setMaxInactiveInterval(7200);

		/*-----------------------------------------------------------------*/

		Map<String, String> token = new HashMap<>();

		token.put("AMIUser", AMIUser);
		token.put("clientDN", clientDN != null ? clientDN : "");
		token.put("issuerDN", issuerDN != null ? issuerDN : "");
		token.put("notBefore", notBefore != null ? notBefore : "");
		token.put("notAfter", notAfter != null ? notAfter : "");

		/*-----------------------------------------------------------------*/

		session.setAttribute("token", token);

		/*------------------------------------------------------------------------------------------------------------*/

		return Response.ok(session.getId()).build();

		/*------------------------------------------------------------------------------------------------------------*/
	}

	/*----------------------------------------------------------------------------------------------------------------*/
}
