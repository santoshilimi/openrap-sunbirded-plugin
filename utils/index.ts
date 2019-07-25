import Hashids from "hashids";
import uuid = require("uuid");

export const hash = (text: string): string => {
  let hash = new Hashids(
    text,
    5,
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
  );
  return hash.encode(1).toLowerCase();
};
