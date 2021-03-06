
import * as uuid from 'uuid';

export default class Response {

    static success(id, result) {
        // prepare success response object
        let resObj = {
            "id": id,
            "ver": "1.0",
            "ts": new Date().toISOString(),
            "params": {
                "resmsgid": uuid.v4(),
                "msgid": uuid.v4(),
                "status": "successful",
                "err": null,
                "errmsg": null
            },
            "responseCode": "OK",
            "result": result
        }
        return resObj;
    }

    static error(id, responseCode) {
        // prepare error response object
        let resObj = {};
        if (responseCode === 404) {
            resObj = {
                id: id,
                ver: "1.0",
                ts: new Date().toISOString(),
                params: {
                    resmsgid: uuid.v4(),
                    msgid: uuid.v4(),
                    status: "failed",
                    err: "ERR_DATA_NOT_FOUND",
                    errmsg: "Data not found"
                },
                responseCode: "RESOURCE_NOT_FOUND",
                result: {}
            }
        } else if (responseCode === 400) {
            resObj = {
                id: id,
                ver: "1.0",
                ts: new Date().toISOString(),
                params: {
                    resmsgid: uuid.v4(),
                    msgid: uuid.v4(),
                    status: "failed",
                    err: "ERR_BAD_REQUEST",
                    errmsg: "Error while processing the request "
                },
                responseCode: "CLIENT_ERROR",
                result: {}
            }
        } else {
            resObj = {
                id: id,
                ver: "1.0",
                ts: new Date().toISOString(),
                params: {
                    resmsgid: uuid.v4(),
                    msgid: uuid.v4(),
                    status: "failed",
                    err: "ERR_INTERNAL_SERVER_ERROR",
                    errmsg: "Error while processing the request"
                },
                responseCode: "INTERNAL_SERVER_ERROR",
                result: {}
            }
        }
        return resObj;
    }
}