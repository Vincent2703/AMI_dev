package net.hep.ami;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import org.everit.json.schema.PrimitiveValidationStrategy;
import org.everit.json.schema.Schema;
import org.everit.json.schema.Validator;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.*;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.JwtParser;
import io.jsonwebtoken.security.SignatureAlgorithm;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public final class Utils {

    private static String JWTPublicKeyPath = "/JWTPublicKey.pub";
    private static Pattern patternIsAFilePath = Pattern.compile("^\\/[\\w\\/-]+\\.\\w+");

    private Utils() {}

    // Decode the JWT, using the public key stored in resources (the path is defined in JWTPublicKeyPath juste above)
    public static String decodeJWT(String encodedJWT) throws Exception {
        // 1) Get the the public key
        SignatureAlgorithm signatureAlgo = Jwts.SIG.RS256; //Get RSA
        PublicKey publicKey = null; 
        try {
            InputStream inputStream = Utils.class.getResourceAsStream(JWTPublicKeyPath);
            if(inputStream != null) {
                String JWTPublicKey = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

                // Make a PublicKey object to decode the JWT
                byte[] byteKey = Base64.getMimeDecoder().decode(JWTPublicKey);
                X509EncodedKeySpec X509publicKey = new X509EncodedKeySpec(byteKey);
                KeyFactory kf = KeyFactory.getInstance("RSA");

                publicKey = kf.generatePublic(X509publicKey);
            }else{
                throw new FileNotFoundException(String.format("Public key not found. Please check the filepath: %s", JWTPublicKeyPath));
            }
        }
        catch(Exception error) {
            throw error;
        }

        // 2) Decode the JWT and get the payload
        JwtParser jwtParser = Jwts.parser()
            .verifyWith(publicKey)
            .build();
        try {
            jwtParser.parseSignedClaims(encodedJWT);

            Base64.Decoder decoder = Base64.getUrlDecoder();
            String[] chunks = encodedJWT.split("\\.");
            String payload = new String(decoder.decode(chunks[1]));

            return payload;

        }catch(Exception error) {
            throw new Exception(String.format("Could not verify JWT token integrity: %s", error));
        }
    } 

    /** 
     * Take a JSON input and a shema and use it against the input to check the structure validity
     * @param input
     * @param schemaPath
     * @return boolean
     * @throws Exception
     */
    public static boolean validateJSON(JSONObject input, String schemaPath) throws Exception {
        InputStream inputStream = Utils.class.getResourceAsStream(schemaPath);
        if(inputStream != null) {
            try {
                String jsonStr = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                JSONObject json = new JSONObject(jsonStr);       
                Schema schema = null;
                schema = SchemaLoader.load(json);
                
                Validator validator = Validator.builder()
                    .primitiveValidationStrategy(PrimitiveValidationStrategy.LENIENT)
                    .build();

                validator.performValidation(schema, input);
                return true;
            }catch(Exception error) {
                throw error;
            }
        }else {
            throw new FileNotFoundException(String.format("JSON schema not found. Please check the filepath: %s", schemaPath));
        }

    }

    // Decode a base64 string
    public static String decodeB64(String b64String) {
        byte[] decodedBytes = Base64.getDecoder().decode(b64String);
        String decodedString = new String(decodedBytes, StandardCharsets.UTF_8);
        return decodedString;
    }

    // Is it a file path ?
    public static boolean isAFilePath(String path) {
        return patternIsAFilePath.matcher(path).find();
    }

}
