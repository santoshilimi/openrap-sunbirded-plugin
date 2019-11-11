import { Manifest } from "@project-sunbird/ext-framework-server/models/Manifest";
import * as path from "path";
import * as _ from "lodash";
import { ResourceBundle } from "./controllers/resourceBundle";
import { Organization } from "./controllers/organization";
import { Form } from "./controllers/form";
import { Channel } from "./controllers/channel";
import { Framework } from "./controllers/framework";
import { Page } from "./controllers/page";
import Tenant from "./controllers/tenant";
import Content from "./controllers/content/content";
import Telemetry from "./controllers/telemetry";
import * as proxy from "express-http-proxy";
import ContentDownload from "./controllers/content/contentDownload";
import ContentUpdate from "./controllers/content/contentUpdate";
import * as url from "url";
import config from "./config";
import { logger } from "@project-sunbird/ext-framework-server/logger";
import * as uuid from "uuid";
import { containerAPI } from "OpenRAP/dist/api";
import DesktopApp from "./controllers/appUpdate";

let telemetry;

const proxyUrl = process.env.APP_BASE_URL;
export class Router {
  init(app: any, manifest: Manifest, auth?: any) {
    const telemetryInstance = containerAPI
      .getTelemetrySDKInstance()
      .getInstance();
    const enableProxy = req => {
      logger.debug(`ReqId = "${req.headers["X-msgid"]}": Checking the proxy`);
      let flag = false;
      if (req.get("referer")) {
        const refererUrl = new url.URL(req.get("referer"));
        let pathName = refererUrl.pathname;
        flag = _.startsWith(pathName, "/browse");
      }
      return flag;
    };

    const updateRequestBody = req => {
      logger.debug(
        `ReqId = "${req.headers["X-msgid"]}": Updating request body filters`
      );
      if (_.get(req, "body.request.filters")) {
        req.body.request.filters.compatibilityLevel = {
          "<=": config.get("CONTENT_COMPATIBILITY_LEVEL")
        };
      }
      return req;
    };

    const logTelemetryEvent = (req, res, next) => {
      const startHrTime = process.hrtime();
      res.on("finish", () => {
        const elapsedHrTime = process.hrtime(startHrTime);
        let elapsedTime =
          (elapsedHrTime[0] * 1000 + elapsedHrTime[1] / 1e6) / 1000;
        if (elapsedTime > 1) {
          logger.warn(
            `${req.headers["X-msgid"] || ""} path: ${
              req.path
            } took ${elapsedTime}s`
          );
        }

        if (res.statusCode >= 200 && res.statusCode <= 300) {
          let params: object[] = [
            {
              duration: parseFloat(elapsedTime.toFixed(3))
            },
            {
              protocol: _.toUpper(req.protocol)
            },
            {
              method: req.method
            },
            {
              url: req.originalUrl
            },
            {
              status: res.statusCode
            }
          ];
          let rid = _.get(req, "rid");
          if (rid) {
            params.push({ rid: rid });
          }
          let size = parseInt(res.getHeader("Content-Length"));
          if (size) {
            params.push({ size: size });
          }
          let logEvent = {
            context: {
              env: "openrap-sunbirded-plugin"
            },
            edata: {
              level: "INFO",
              type: "api_access",
              message: `The api is successfully processed with url ${req.originalUrl} and method ${req.method}`,
              params: params
            }
          };
          telemetryInstance.log(logEvent);
        }
      });
      next();
    };
    app.use(logTelemetryEvent);
    const addRequestId = (req, res, next) => {
      req.headers["X-msgid"] = req.get("X-msgid") || uuid.v4();
      next();
    };
    app.use(addRequestId);
    //portal static routes
    app.all(
      [
        "/",
        "/play/*",
        "/import/content",
        "/get",
        "/get/*",
        "/browse",
        "/browse/*",
        "/search/*",
        "/help-center",
        "/help-center/*"
      ],
      async (req, res) => {
        const locals = await this.getLocals(manifest);
        _.forIn(locals, (value, key) => {
          res.locals[key] = value;
        });
        res.render(
          path.join(__dirname, "..", "..", "public", "portal", "index.ejs")
        );
      }
    );

    // api's for portal

    let resourcebundle = new ResourceBundle(manifest);
    app.get("/resourcebundles/v1/read/:id", (req, res) => {
      resourcebundle.get(req, res);
    });

    let organization = new Organization(manifest);
    app.post(
      "/api/org/v1/search",
      (req, res, next) => {
        logger.debug(`Received API call to search organisations`);

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          next();
        } else {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Search organisations`
          );
          return organization.search(req, res);
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/org/v1/search`;
        }
      })
    );

    let form = new Form(manifest);
    app.post(
      "/api/data/v1/form/read",
      (req, res, next) => {
        logger.debug(`Received API call to read formdata`);

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          next();
        } else {
          logger.debug(`ReqId = "${req.headers["X-msgid"]}": Search form data`);
          return form.search(req, res);
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/data/v1/form/read`;
        }
      })
    );

    let channel = new Channel(manifest);
    app.get(
      "/api/channel/v1/read/:id",
      (req, res, next) => {
        logger.debug(
          `Received API call to get channel data for channel with Id: ${req.params.id}`
        );

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          next();
        } else {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Get channel data for channel with Id:${req.params.id}`
          );
          return channel.get(req, res);
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/channel/v1/read/${req.params.id}`;
        }
      })
    );

    let framework = new Framework(manifest);
    app.get(
      "/api/framework/v1/read/:id",
      (req, res, next) => {
        logger.debug(
          `Received API call to get framework data for framework with Id: ${req.params.id}`
        );

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          next();
        } else {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Get Framework data for Framework with Id:${req.params.id}`
          );
          return framework.get(req, res);
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/framework/v1/read/${req.params.id}`;
        }
      })
    );

    let page = new Page(manifest);
    app.post(
      "/api/data/v1/page/assemble",
      (req, res, next) => {
        logger.debug(`Received API call to page asemble`);

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Update requestbody`
          );
          req = updateRequestBody(req);
          logger.info(
            `ReqId = "${req.headers["X-msgid"]}": Request body filters updated successfully`
          );
          next();
        } else {
          logger.debug(`ReqId = "${req.headers["X-msgid"]}": Get page data`);
          return page.get(req, res);
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/data/v1/page/assemble`;
        },
        userResDecorator: function(proxyRes, proxyResData, req) {
          return new Promise(function(resolve) {
            logger.info(`Proxy is Enabled for Content`);
            logger.debug(
              `ReqId = "${req.headers["X-msgid"]}": Convert buffer data to json`
            );
            const proxyData = content.convertBufferToJson(proxyResData, req);
            let sections = _.get(proxyData, "result.response.sections");
            if (!_.isEmpty(sections)) {
              logger.debug(
                `ReqId = "${req.headers["X-msgid"]}": Calling decorateSections to decorate a content`
              );
              content
                .decorateSections(sections, req.headers["X-msgid"])
                .then(() => {
                  logger.info(
                    `ReqId = "${req.headers["X-msgid"]}": Resolving Data after decorating content `
                  );
                  resolve(proxyData);
                })
                .catch(err => {
                  logger.error(
                    `ReqId = "${req.headers["X-msgid"]}": Received error err.message`,
                    err
                  );
                  resolve(proxyData);
                });
            } else {
              logger.info(
                `ReqId = "${req.headers["X-msgid"]}": Resolving data if there in no content in page assemble request`
              );
              resolve(proxyData);
            }
          });
        }
      })
    );

    let tenant = new Tenant();
    app.get(
      ["/v1/tenant/info/", "/v1/tenant/info/:id"],
      (req, res, next) => {
        logger.debug(
          `Received API call to get tenant data ${_.upperCase(
            _.get(req, "params.id")
          )}`
        );

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          next();
        } else {
          logger.debug(`ReqId = "${req.headers["X-msgid"]}": Get tenant Info`);
          tenant.get(req, res);
          return;
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/v1/tenant/info/`;
        }
      })
    );

    let content = new Content(manifest);
    app.get(
      "/api/content/v1/read/:id",
      (req, res, next) => {
        logger.debug(`Received API call to read Content: ${req.params.id}`);

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled`);
          next();
        } else {
          logger.info(`ReqId = "${req.headers["X-msgid"]}": Proxy is disabled`);
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Calling Content get method to get Content: ${req.params.id} `
          );
          content.get(req, res);
          return;
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/content/v1/read/${req.params.id}?fields=${req.query.fields}`;
        },
        userResDecorator: function(proxyRes, proxyResData, req) {
          return new Promise(function(resolve) {
            logger.info(`Proxy is Enabled for Content: ${req.params.id}`);
            logger.debug(
              `ReqId = "${req.headers["X-msgid"]}": Convert buffer data to json`
            );
            const proxyData = content.convertBufferToJson(proxyResData, req);
            let contents = _.get(proxyData, "result.content");
            if (!_.isEmpty(contents)) {
              logger.debug(
                `ReqId = "${req.headers["X-msgid"]}": Calling decorateContent to decorate a content`
              );
              content
                .decorateContentWithProperty([contents], req.headers["X-msgid"])
                .then(() => {
                  logger.info(
                    `ReqId = "${req.headers["X-msgid"]}": Resolving Data after decorating content `
                  );
                  resolve(proxyData);
                })
                .catch(err => {
                  logger.error(
                    `ReqId = "${req.headers["X-msgid"]}": Received error err.message`,
                    err
                  );
                  resolve(proxyData);
                });
            } else {
              logger.info(
                `ReqId = "${req.headers["X-msgid"]}": Resolving data if there in no content in request`
              );
              resolve(proxyData);
            }
          });
        }
      })
    );

    app.get(
      "/api/course/v1/hierarchy/:id",
      (req, res, next) => {
        logger.debug(
          `Received API call to get Course hierarchy: ${req.params.id}`
        );

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled`);
          next();
        } else {
          logger.info(`ReqId = "${req.headers["X-msgid"]}": Proxy is disabled`);
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Calling Content get method to get CourseHierarchy: ${req.params.id} `
          );
          content.get(req, res);
          return;
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/course/v1/hierarchy/${req.params.id}`;
        },
        userResDecorator: function(proxyRes, proxyResData, req) {
          return new Promise(function(resolve) {
            logger.info(`Proxy is Enabled for Content: ${req.params.id}`);
            logger.debug(
              `ReqId = "${req.headers["X-msgid"]}": Convert buffer data to json`
            );
            const proxyData = content.convertBufferToJson(proxyResData, req);
            let contents = _.get(proxyData, "result.content");
            if (!_.isEmpty(contents)) {
              logger.debug(
                `ReqId = "${req.headers["X-msgid"]}": Calling decorateDialCodeContent to decorate a content`
              );
              content
                .decorateDialCodeContents(contents, req.headers["X-msgid"])
                .then(() => {
                  logger.info(
                    `ReqId = "${req.headers["X-msgid"]}": Resolving Data after decorating DialCodecontent `
                  );
                  resolve(proxyData);
                })
                .catch(err => {
                  logger.error(
                    `ReqId = "${req.headers["X-msgid"]}": Received error err.message`,
                    err
                  );
                  resolve(proxyData);
                });
            } else {
              logger.info(
                `ReqId = "${req.headers["X-msgid"]}": Resolving data if there in no content in course hierarchy request`
              );
              resolve(proxyData);
            }
          });
        }
      })
    );

    app.post(
      "/api/content/v1/search",
      (req, res, next) => {
        logger.debug(`Received API call to search content`);

        logger.debug(`ReqId = "${req.headers["X-msgid"]}": Check proxy`);
        if (enableProxy(req)) {
          logger.info(`Proxy is Enabled `);
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Update requestbody`
          );
          req = updateRequestBody(req);
          logger.info(
            `ReqId = "${req.headers["X-msgid"]}": Request body filters updated successfully`
          );
          next();
        } else {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Calling content search method`
          );
          content.search(req, res);
          return;
        }
      },
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          return `/api/content/v1/search`;
        },
        userResDecorator: function(proxyRes, proxyResData, req) {
          return new Promise(function(resolve) {
            logger.info(`Proxy is Enabled for Content`);
            logger.debug(
              `ReqId = "${req.headers["X-msgid"]}": Convert buffer data to json`
            );
            const proxyData = content.convertBufferToJson(proxyResData, req);
            let contents = _.get(proxyData, "result.content");
            if (!_.isEmpty(contents)) {
              logger.debug(
                `ReqId = "${req.headers["X-msgid"]}": Calling decorateContent to decorate contents in contentSearch`
              );
              content
                .decorateContentWithProperty(contents, req.headers["X-msgid"])
                .then(() => {
                  logger.info(
                    `ReqId = "${req.headers["X-msgid"]}": Resolving Data after decorating contents in contentSearch `
                  );
                  resolve(proxyData);
                })
                .catch(err => {
                  logger.error(
                    `ReqId = "${req.headers["X-msgid"]}": Received error err.message`,
                    err
                  );
                  resolve(proxyData);
                });
            } else {
              logger.info(
                `ReqId = "${req.headers["X-msgid"]}": Resolving data if there in no content in contentSearch request`
              );
              resolve(proxyData);
            }
          });
        }
      })
    );
    app.post("/api/content/v1/import", content.import.bind(content));
    app.post(
      "/api/content/v1/import/pause/:importId",
      content.pauseImport.bind(content)
    );
    app.post(
      "/api/content/v1/import/resume/:importId",
      content.resumeImport.bind(content)
    );
    app.post(
      "/api/content/v1/import/cancel/:importId",
      content.cancelImport.bind(content)
    );

    app.get(
      "/api/content/v1/export/:id",
      this.setConnectionTimeout(1200000),
      content.export.bind(content)
    );

    let contentDownload = new ContentDownload(manifest);
    app.post("/api/content/v1/download/list", (req, res) => {
      contentDownload.list(req, res);
    });
    app.post("/api/content/v1/download/:id", (req, res) => {
      contentDownload.download(req, res);
    });

    telemetry = new Telemetry(manifest);

    app.post(
      ["/content/data/v1/telemetry", "/action/data/v3/telemetry"],
      (req, res) => {
        telemetry.addEvents(req, res);
      }
    );
    let contentUpdate = new ContentUpdate(manifest);
    app.post("/api/content/v1/update/:id", (req, res) => {
      contentUpdate.contentUpdate(req, res);
    });

    let desktopApp = new DesktopApp();
    app.get("/api/desktop/v1/update", (req, res) => {
      desktopApp.getDesktopAppUpate(req, res);
    });

    app.use(
      "/content-plugins/*",
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Parsing content-plugin urls`
          );
          return require("url").parse(proxyUrl + req.originalUrl).path;
        }
      })
    );

    app.use(
      "/assets/public/*",
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Parsing assets/public urls`
          );
          return require("url").parse(proxyUrl + req.originalUrl).path;
        }
      })
    );

    app.use(
      "/contentPlayer/preview",
      proxy(proxyUrl, {
        proxyReqPathResolver: function(req) {
          logger.debug(
            `ReqId = "${req.headers["X-msgid"]}": Parsing contentPlayer/preview/ urls`
          );
          return require("url").parse(proxyUrl + req.originalUrl).path;
        }
      })
    );
  }

  setConnectionTimeout(time: Number) {
    return (req, res, next) => {
      req.connection.setTimeout(time);
      next();
    };
  }

  async getLocals(manifest) {
    let deviceId = await containerAPI
      .getSystemSDKInstance(manifest.id)
      .getDeviceId();
    var locals: any = {};
    locals.userId = null;
    locals.sessionId = null;
    locals.cdnUrl = "";
    locals.theme = "";
    locals.defaultPortalLanguage = "en";
    locals.instance = process.env.APP_NAME;
    locals.appId = process.env.APP_ID;
    locals.defaultTenant = process.env.CHANNEL || "sunbird";
    locals.exploreButtonVisibility = "true";
    locals.helpLinkVisibility = null;
    locals.defaultTenantIndexStatus = null;
    locals.extContWhitelistedDomains = null;
    locals.buildNumber = process.env.APP_VERSION;
    locals.apiCacheTtl = "5";
    locals.cloudStorageUrls = null;
    locals.userUploadRefLink = null;
    locals.googleCaptchaSiteKey = null;
    locals.videoMaxSize = null;
    locals.reportsLocation = null;
    locals.deviceRegisterApi = "/api/v1/device/registry/";
    locals.playerCdnEnabled = "";
    locals.previewCdnUrl = "";
    locals.cdnWorking = null;
    locals.offlineDesktopAppTenant = "";
    locals.offlineDesktopAppVersion = "";
    locals.offlineDesktopAppReleaseDate = "";
    locals.offlineDesktopAppSupportedLanguage = "";
    locals.offlineDesktopAppDownloadUrl = "";
    locals.logFingerprintDetails = "";
    locals.deviceId = deviceId;
    locals.deviceProfileApi =""
    return locals;
  }
}
