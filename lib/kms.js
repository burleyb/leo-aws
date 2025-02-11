"use strict";
const { KMSClient, DecryptCommand, EncryptCommand } = require("@aws-sdk/client-kms");

module.exports = function(configuration) {
    const client = new KMSClient({ region: configuration.region });
    
    return {
        _service: client,
        decrypt: async function(encryptedString) {
            const command = new DecryptCommand({
                CiphertextBlob: Buffer.from(encryptedString, 'base64'),
            });
            
			try {
            	const response = await client.send(command);
            	return Buffer.from(response.Plaintext).toString('ascii');
			} catch (e) {
				console.log("error", e)
				throw e;
			}
        },
        encrypt: async function(key, value) {
            const command = new EncryptCommand({
                KeyId: key,
                Plaintext: Buffer.from(value)
            });
            
            const response = await client.send(command);
            return Buffer.from(response.CiphertextBlob).toString("base64");
        }
    };
};
