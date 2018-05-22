'use strict'

const uniqid = require('uniqid');
const fs = require('fs');
const path = require('path');
const q = require('q');

const { config } = require('../config');
const deviceID = config.did;


// const {
// 	saveTelemetry
// } = require('../../telemetrySdk');

// Generic telemetry JSON structure
let telemetryData = {
	'eid': '',
	'ets': '',
	'ver': '',
	'mid': '',
	'actor': {
		'id': '',
		'type': ''
	},
	'context': {
		'channel': '',
		'pdata': {
			'id': '',
			'pid': '',
			'ver': ''
		},
		'env': '',
		'sid': '',
		'did': '',
		'cdata': [{
			'type':'',
			'id': ''
		}],
		'rollup': {
			'l1': '',
			'l2': '',
			'l3': '',
			'l4': ''
		}
	},
	'object': {
		'id': '',
		'type': '',
		'ver': '',
		'rollup': {
			'l1': '',
			'l2': '',
			'l3': '',
			'l4': ''
		}
	},
	'edata': {
		'event' : '',
		'value': '',
		'actor' : '',
		'actorDetails': ''
	},
	'tags': ['']
}

const formatBytes = (bytes, decimals) => {
	if(bytes === 0) return '0 Bytes';
	const k = 1024,
		dm = decimals || 2,
		sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
		i = Math.floor(Math.log(bytes) / Math.log(k));

	return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

const _getSystemVersion = () => {
	let defer = q.defer();

	const cdn = path.join(__dirname, '../../CDN/version.txt');

	fs.readFile(cdn, 'utf-8', (err, data) => {
		if(err) {
			defer.reject(err);
		} else {
			defer.resolve(data);
		}
	});

    return defer.promise;
}

const saveTelemetryData = (req, res, next) => {

	const actor = req.body.actor || req.query.actor || req.params['actor'];
	const timestamp = new Date().toLocaleString('en-IN').replace(',', '');

	switch(req.route.path) {
		case '/file/new' :
			telemetryData = {
				...telemetryData,
				'eid' : 'LOG',
				'edata' : {
					'type' : 'api_call',
					'level' : 'INFO',
					'message' : 'Created new file',
					'params' : [
						{
							timestamp,
							actor,
							'details' : {
								'name' : req.files.file.name,
								'size' : formatBytes(req.files.file.size),
								'type' : req.files.file.type
							}
						}
					]
				}
			}

			break;

		case '/file/delete' :
			telemetryData = {
				...telemetryData,
				'eid' : 'LOG',
				'edata' : {
					'type' : 'api_call',
					'level' : 'INFO',
					'message' : 'Deleted file/folder',
					'params' : [
						{
							timestamp,
							actor,
							'path' : decodeURIComponent(req.query.path)
						}
					]
				}
			}

			break;

		case '/file/newFolder' :
			telemetryData = {
				...telemetryData,
				'eid' : 'LOG',
				'edata' : {
					'type' : 'api_call',
					'level' : 'INFO',
					'message' : 'Created new folder',
					'params' : [
						{
							timestamp,
							actor,
							'path' : decodeURIComponent(req.body.path)
						}
					]
				}
			}

			break;

		case '/ssid/set' :
			telemetryData = {
				...telemetryData,
				'eid' : 'AUDIT',
				'edata' : {
					'props': ['ssid'],
					'state': req.body['ssid'].trim(),
					'prevstate': ''
				}
			}

			break;

		case '/user/create' :
			telemetryData = {
				...telemetryData,
				'eid' : 'LOG',
				'edata' : {
					'type' : 'api_call',
					'level' : 'INFO',
					'message' : 'User added',
					'params' : [
						{
							timestamp,
							actor,
							'user' : req.body.username
						}
					]
				}
			}

			break;

		case '/user/delete/:username' :
			telemetryData = {
				...telemetryData,
				'eid' : 'LOG',
				'edata' : {
					'type' : 'api_call',
					'level' : 'INFO',
					'message' : 'User removed',
					'params' : [
						{
							timestamp,
							actor,
							'user' : req.params['username']
						}
					]
				}
			}

			break;

		case '/user/update' :
			telemetryData = {
				...telemetryData,
				'eid' : 'AUDIT',
				'edata' : {
					'props': ['permisions'],
					'state': {
						'username' : req.body.username,
						'permissions' : JSON.parse(req.body.value)
					},
					'prevstate': {
						'username' : req.body.username,
						'permissions' : req.body.oldValue
					}
				}
			}

			break;
	}

	_getSystemVersion()
		.then(systemVersion => {
			telemetryData = {
				...telemetryData,
				'ets' : new Date().getTime(),
				'ver' : '3.0',
				'mid' : uniqid(`${deviceID}-`), // TODO : Use appropriate device ID
				'actor' : {
					'id' : actor
				},
				'context': {
					'channel' : 'OpenRAP',
					'pdata' : {
						'id' : deviceID,
 						'pid' : require('process').pid,
						'ver' : systemVersion.replace(/\n$/, '')
					},
					'env' : 'Device Management'
				}
			}
			
			console.log(JSON.stringify(telemetryData, null, 4));

			// saveTelemetry(telemetryData, 'devmgmt');

			next();
		})
		.catch((err) => console.log(err));
}

module.exports = {
	saveTelemetryData
}
