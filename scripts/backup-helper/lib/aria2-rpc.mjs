import Aria2 from 'aria2'

/**
 * aria2.js dispatches JSON-RPC notifications through EventTarget and attaches
 * the JSON-RPC `params` payload on the event object.
 *
 * @template T
 * @typedef {Event & { params: T }} Aria2RPCNotification
 */

/**
 * Status values returned by `aria2.tellStatus(gid, ['status'])`.
 *
 * - `active`: currently downloading or seeding.
 * - `waiting`: in the queue and not started yet.
 * - `paused`: currently paused.
 * - `error`: stopped because of an error.
 * - `complete`: stopped and completed.
 * - `removed`: removed by the user.
 *
 * @typedef {"active" | "waiting" | "paused" | "error" | "complete" | "removed"} Aria2DownloadStatus
 */

/**
 * Allowed keys for the optional `keys` filter passed to `aria2.tellStatus()`.
 *
 * - `gid`: GID of the download.
 * - `status`: Download status. See `Aria2DownloadStatus`.
 * - `totalLength`: Total length of the download in bytes.
 * - `completedLength`: Completed length of the download in bytes.
 * - `uploadLength`: Uploaded length of the download in bytes.
 * - `bitfield`: Hexadecimal representation of the download progress.
 * - `downloadSpeed`: Download speed in bytes/sec.
 * - `uploadSpeed`: Upload speed in bytes/sec.
 * - `infoHash`: InfoHash. BitTorrent only.
 * - `numSeeders`: Number of connected seeders. BitTorrent only.
 * - `seeder`: `true` if the local endpoint is a seeder. BitTorrent only.
 * - `pieceLength`: Piece length in bytes.
 * - `numPieces`: Number of pieces.
 * - `connections`: Number of connected peers/servers.
 * - `errorCode`: Error code string. Only available for stopped/completed downloads.
 * - `errorMessage`: Human-readable message associated with `errorCode`.
 * - `followedBy`: List of GIDs generated as a result of this download.
 * - `following`: Reverse link for `followedBy`.
 * - `belongsTo`: GID of a parent download.
 * - `dir`: Directory used to save files.
 * - `files`: List of files, using the same structs returned by `aria2.getFiles()`.
 * - `bittorrent`: Struct containing torrent metadata. BitTorrent only.
 * - `verifiedLength`: Number of verified bytes during a hash check.
 * - `verifyIntegrityPending`: `true` if the download is waiting for a hash check in the queue.
 *
 * @typedef {"gid" | "status" | "totalLength" | "completedLength" | "uploadLength" | "bitfield"
 *  | "downloadSpeed" | "uploadSpeed" | "infoHash" | "numSeeders" | "seeder" | "pieceLength"
 *  | "numPieces" | "connections" | "errorCode" | "errorMessage" | "followedBy" | "following"
 *  | "belongsTo" | "dir" | "files" | "bittorrent" | "verifiedLength" | "verifyIntegrityPending"
 * } Aria2StatusKey
 */

/**
 * Thin wrapper around the aria2 JSON-RPC client used by backup-helper.
 *
 * This wrapper intentionally exposes only the RPC surface this repo currently
 * uses and documents each method against the local aria2 RPC reference.
 */
export class Aria2RPC {
  /**
   * @param {ConstructorParameters<typeof Aria2>[0]} [options]
   */
  constructor(options = {}) {
    this.client = new Aria2(options)
  }

  /**
   * Opens the JSON-RPC over WebSocket transport.
   *
   * Notifications such as `aria2.onDownloadComplete` and
   * `aria2.onDownloadError` are only available over WebSocket.
   */
  open() {
    return this.client.open()
  }

  /**
   * Closes the WebSocket transport.
   *
   * After close, the underlying aria2.js client would fall back to HTTP for
   * direct RPC calls, but backup-helper currently requires WebSocket
   * notifications to operate.
   */
  close() {
    return this.client.close()
  }

  /**
   * aria2.tellStatus(gid[, keys])
   *
   * Returns the progress of the download denoted by `gid`.
   *
   * @param {string} gid - The download identifier assigned by aria2.
   * @param {Aria2StatusKey[]} [keys] - If provided, the response contains only the
   * keys listed in that array. If `keys` is omitted or an empty array, aria2
   * returns all available keys for the download. This is useful when you only
   * need a subset of fields and want to avoid unnecessary transfer.
   */
  tellStatus(gid, keys) {
    return this.client.call('tellStatus', gid, keys)
  }

  /**
   * system.multicall(calls)
   *
   * Encapsulates multiple RPC method calls in a single request.
   *
   * `calls` is an array of tuples. Each tuple is shaped like:
   * - [methodName, ...params]
   *
   * @param {[string, ...unknown[]][]} calls
   * @example
   * const calls = [
   *   ['addUri', ['http://example.org/file-a'], { dir: '/tmp' }],
   *   ['tellStatus', '2089b05ecca3d829', ['gid', 'status']],
   * ]
   *
   * const results = await rpc.multicall(calls)
   */
  multicall(calls) {
    return this.client.multicall(calls)
  }

  /**
   * aria2.shutdown()
   *
   * Requests a graceful aria2 shutdown. This returns "OK" on success.
   */
  shutdown() {
    return this.client.call('shutdown')
  }

  /**
   * Attach a WebSocket notification listener such as `onDownloadComplete` or
   * `onDownloadError`.
   *
   * aria2.js delivers notifications as EventTarget events and exposes the
   * JSON-RPC payload on `event.params`.
   *
   * @template T
   * @param {string} eventName
   * @param {(event: Aria2RPCNotification<T>) => void} listener
   */
  addEventListener(eventName, listener) {
    this.client.addEventListener(eventName, listener)
  }
}
