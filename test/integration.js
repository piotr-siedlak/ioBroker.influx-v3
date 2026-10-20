'use strict';
/**
 * Integration tests with a real js-controller (@iobroker/testing).
 *
 * Always: the adapter starts, creates its objects and stops cleanly (also without a database).
 * With INFLUX3_NOAUTH_URL (InfluxDB 3 Core started with --without-auth, see scripts/influx3-test-server.sh)
 * or INFLUX3_URL + INFLUX3_TOKEN: end-to-end tests of per-state configuration, tags, live re-configuration,
 * enableHistory/disableHistory, getHistory, SQL queries and multi-instance isolation.
 */
const path = require('node:path');
const http = require('node:http');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const { tests } = require('@iobroker/testing');
const ioPackage = require('../io-package.json');

const URL = process.env.INFLUX3_NOAUTH_URL || process.env.INFLUX3_URL || '';
// old InfluxDB 2.x of the classic adapter (scripts/legacy-influx-test-server.sh) for the import tests
const V2 = process.env.INFLUX2_URL && process.env.INFLUX2_TOKEN ? process.env : null;
const TOKEN = process.env.INFLUX3_NOAUTH_URL ? '' : process.env.INFLUX3_TOKEN || '';
const DB = 'iobharness';
// unique per run, so data of earlier runs in the same database cannot interfere
const BASE = `0_userdata.0.iv3_${Date.now().toString(36)}`;

function sql(query, db = DB) {
    return new Promise((resolve, reject) => {
        const u = new globalThis.URL(`${URL}/api/v3/query_sql`);
        const body = JSON.stringify({ db, q: query, format: 'json' });
        const req = http.request(
            {
                hostname: u.hostname,
                port: u.port,
                path: u.pathname,
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'Content-Length': Buffer.byteLength(body),
                    ...(TOKEN ? { Authorization: `Bearer ${TOKEN}` } : {}),
                },
            },
            res => {
                let data = '';
                res.on('data', c => (data += c));
                res.on('end', () => (res.statusCode === 200 ? resolve(JSON.parse(data || '[]')) : reject(new Error(`${res.statusCode} ${data}`))));
            },
        );
        req.on('error', reject);
        req.end(body);
    });
}

const sleep = ms => new Promise(r => setTimeout(r, ms));

async function waitFor(fn, timeoutMs = 15000) {
    const start = Date.now();
    for (;;) {
        try {
            const v = await fn();
            if (v) {
                return v;
            }
        } catch {
            // retry
        }
        if (Date.now() - start > timeoutMs) {
            throw new Error('condition not reached in time');
        }
        await sleep(300);
    }
}

/**
 * sendTo that matches the reply by its callback id. harness.sendTo() hands the next reply to whichever call waits,
 * so it cannot be used for parallel requests (like the many cells of an Admin table).
 */
let matchedId = 1_000_000;
function sendToMatched(harness, command, message, instance = 'influx-v3.0') {
    return new Promise(resolve => {
        const id = matchedId++;
        const handler = (sid, state) => {
            if (sid === 'messagebox.system.adapter.test.0' && state?.callback?.id === id) {
                harness.removeListener('stateChange', handler);
                resolve(state.message);
            }
        };
        harness.addListener('stateChange', handler);
        harness.dbConnection.pushMessage(`system.adapter.${instance}`, {
            command,
            message,
            from: 'system.adapter.test.0',
            callback: { message, id, ack: false, time: Date.now() },
        });
    });
}

function sendTo(harness, command, message, instance = 'influx-v3.0') {
    return new Promise(resolve => harness.sendTo(instance, command, message, resolve));
}

/** tag catalog used by the end-to-end tests */
const CATALOG = [
    { key: 'Location', values: 'KOTLOWNIA, SALON', allowOther: false, defaultValue: '', description: 'room of the sensor' },
    { key: 'Sub-Type', values: 'HC1, HC2, HC3', allowOther: false, defaultValue: '', description: '' },
    { key: 'device', values: '', allowOther: true, defaultValue: '', description: 'templates allowed' },
    { key: 'kind', values: 'early, late', allowOther: false, defaultValue: '', description: '' },
    { key: 'inst', values: 'zero, one', allowOther: false, defaultValue: '', description: '' },
    { key: 'site', values: 'home, second', allowOther: false, defaultValue: 'home', description: '' },
];

function stateObject(type, custom, extra = {}) {
    return {
        type: 'state',
        common: { name: `test ${type}`, type, role: type === 'number' ? 'value.temperature' : 'state', read: true, write: true, custom, ...extra },
        native: {},
    };
}

tests.integration(path.join(__dirname, '..'), {
    allowedExitCodes: [11],
    defineAdditionalTests({ suite }) {
        suite('Start without a reachable database', getHarness => {
            it('starts, reports info.connection=false and stops cleanly', async function () {
                this.timeout(60000);
                const harness = getHarness();
                await harness.changeAdapterConfig('influx-v3', { native: { url: 'http://127.0.0.1:1', token: 'apiv3_unit', pingIntervalSec: 10 } });
                await harness.startAdapterAndWait();
                await sleep(2000);
                const conn = await harness.states.getStateAsync('influx-v3.0.info.connection');
                assert.equal(conn && conn.val, false);
                const obj = await harness.objects.getObjectAsync('influx-v3.0.info.queueRecords');
                assert.ok(obj, 'instance objects created');
                // the token is stored encrypted and never logged
                const inst = await harness.objects.getObjectAsync('system.adapter.influx-v3.0');
                assert.notEqual(inst.native.token, 'apiv3_unit');
                assert.ok(!harness.getLogs().some(l => l.message.includes('apiv3_unit')), 'token must not appear in logs');
                await harness.stopAdapter();
            });
        });

        suite('Former global tags are moved into the tag catalog', getHarness => {
            it('converts globalTags once and saves them as tagDefinitions', async function () {
                this.timeout(60000);
                const harness = getHarness();
                await harness.changeAdapterConfig('influx-v3', {
                    native: { url: 'http://127.0.0.1:1', token: 'apiv3_unit', globalTags: [{ key: 'site', value: 'home' }], tagDefinitions: [] },
                });
                await harness.startAdapterAndWait();
                const inst = await waitFor(async () => {
                    const o = await harness.objects.getObjectAsync('system.adapter.influx-v3.0');
                    return o.native.tagDefinitions.length ? o : null;
                });
                assert.deepEqual(inst.native.tagDefinitions, [
                    { key: 'site', values: '', allowOther: true, defaultValue: 'home', description: '' },
                ]);
                assert.deepEqual(inst.native.globalTags, []);
                assert.ok(harness.getLogs().some(l => l.message.includes('moved into the tag catalog')));
                await harness.stopAdapter();
            });
        });

        (URL ? suite : () => undefined)('End-to-end with InfluxDB 3', getHarness => {
            let harness;
            const objects = {
                num: `${BASE}.num`,
                bool: `${BASE}.bool`,
                str: `${BASE}.str`,
                other: `${BASE}.otherInstance`,
                later: `${BASE}.later`,
            };
            const classicCustom = { enabled: true, debounceTime: 0, customTags: [{ name: 'legacy', value: 'yes' }] };

            before(async function () {
                this.timeout(120000);
                harness = getHarness();
                await harness.objects.setObjectAsync('enum.rooms.boiler', {
                    type: 'enum',
                    common: { name: 'Boiler room', members: [BASE] },
                    native: {},
                });
                await harness.objects.setObjectAsync(BASE, { type: 'channel', common: { name: 'Heating circuit 1' }, native: {} });
                await harness.objects.setObjectAsync(
                    objects.num,
                    stateObject(
                        'number',
                        {
                            'influx-v3.0': {
                                enabled: true,
                                changesOnly: false,
                                customTags: [
                                    { key: 'Location', value: 'KOTLOWNIA' },
                                    { key: 'Sub-Type', value: 'HC1' },
                                    { key: 'device', value: '{id.segment[2]}' },
                                ],
                            },
                            'influxdb.0': classicCustom,
                        },
                        { unit: '°C' },
                    ),
                );
                await harness.objects.setObjectAsync(objects.bool, stateObject('boolean', { 'influx-v3.0': { enabled: true, changesOnly: false } }));
                await harness.objects.setObjectAsync(objects.str, stateObject('string', { 'influx-v3.0': { enabled: true } }));
                await harness.objects.setObjectAsync(objects.other, stateObject('number', { 'influx-v3.1': { enabled: true } }));
                await harness.objects.setObjectAsync(objects.later, stateObject('number', {}));
                await harness.changeAdapterConfig('influx-v3', {
                    native: {
                        url: URL,
                        token: TOKEN,
                        database: DB,
                        flushIntervalMs: 200,
                        tagDefinitions: CATALOG,
                        autoTagChannel: true,
                        storeOnStart: false,
                    },
                });
                await harness.startAdapterAndWait();
                await waitFor(async () => (await harness.states.getStateAsync('influx-v3.0.info.connection'))?.val === true);
            });

            it('registers only the states configured for this instance', async function () {
                this.timeout(20000);
                // the harness database may still contain objects of earlier runs → look at this run only
                const ours = r => Object.keys(r).filter(id => id.startsWith(`${BASE}.`));
                const dps = await waitFor(async () => {
                    const r = await sendTo(harness, 'getEnabledDPs', {});
                    return ours(r).length === 3 ? r : null;
                });
                assert.deepEqual(ours(dps).sort(), [objects.bool, objects.num, objects.str].sort());
                assert.equal(dps[objects.num].table, 'numeric_states');
            });

            it('writes values with timestamp, layered tags and metadata', async function () {
                this.timeout(30000);
                await harness.states.setStateAsync(objects.num, { val: 21.5, ack: true, ts: Date.now() });
                await harness.states.setStateAsync(objects.bool, { val: true, ack: false });
                await harness.states.setStateAsync(objects.str, { val: 'hello "world"', ack: true });
                const rows = await waitFor(async () => {
                    const r = await sql(`SELECT * FROM numeric_states WHERE state_id = '${objects.num}'`);
                    return r.length ? r : null;
                });
                const row = rows[0];
                assert.equal(row.value, 21.5);
                assert.equal(row.location, 'KOTLOWNIA');
                assert.equal(row.sub_type, 'HC1');
                assert.equal(row.device, BASE.split('.')[2]);
                assert.equal(row.site, 'home');
                assert.equal(row.room, 'Boiler room');
                assert.equal(row.channel, 'Heating circuit 1');
                assert.equal(row.ack, true);
                assert.equal(row.q, 0);
                assert.equal(row.legacy, undefined, 'tags of influxdb.0 are not used');
                const b = await waitFor(async () => (await sql(`SELECT value, ack FROM boolean_states WHERE state_id = '${objects.bool}'`))[0]);
                assert.deepEqual(b, { value: true, ack: false });
                const s = await waitFor(async () => (await sql(`SELECT value FROM string_states WHERE state_id = '${objects.str}'`))[0]);
                assert.equal(s.value, 'hello "world"');
                const other = await sql(`SELECT count(*) AS n FROM numeric_states WHERE state_id = '${objects.other}'`);
                assert.equal(Number(other[0].n), 0, 'state of influx-v3.1 must not be written by influx-v3.0');
            });

            it('getHistory returns the stored values', async function () {
                this.timeout(20000);
                const res = await sendTo(harness, 'getHistory', {
                    id: objects.num,
                    options: { start: Date.now() - 3600000, end: Date.now() + 1000, aggregate: 'none', ack: true },
                });
                assert.equal(res.error, null);
                assert.ok(res.result.some(e => e.val === 21.5 && e.ack === true));
            });

            it('previewMapping shows table, tags and line protocol without saving', async function () {
                this.timeout(20000);
                // Admin sends the complete (unsaved) form data of the custom dialog
                const obj = await harness.objects.getObjectAsync(objects.num);
                const config = { ...obj.common.custom['influx-v3.0'], table: 'heating' };
                const res = await sendTo(harness, 'previewMapping', { id: objects.num, config });
                assert.match(res.text, /Table: heating/);
                assert.match(res.text, /location=KOTLOWNIA/);
                assert.match(res.text, /sub_type=HC1/);
                const dps = await sendTo(harness, 'getEnabledDPs', {});
                assert.equal(dps[objects.num].table, 'numeric_states', 'preview does not change the configuration');
            });

            it('the object dialog gets keys and values from the tag catalog', async function () {
                this.timeout(20000);
                const keys = await sendTo(harness, 'getTagKeys', {});
                assert.deepEqual(
                    keys.map(k => k.value),
                    CATALOG.map(d => d.key),
                );
                assert.equal(keys[0].label, 'Location – room of the sensor');
                const values = await sendTo(harness, 'getTagValues', { key: 'site' });
                assert.deepEqual(values, [
                    { value: 'home', label: 'home (default)' },
                    { value: 'second', label: 'second' },
                ]);
                assert.deepEqual(await sendTo(harness, 'getTagValues', { key: 'unknown' }), []);
            });

            it('tags outside the catalog are ignored (with warnings), catalog tags are written', async function () {
                this.timeout(30000);
                const res = await sendTo(harness, 'enableHistory', {
                    id: objects.later,
                    options: {
                        changesOnly: false,
                        customTags: [
                            { key: 'kind', value: 'early' },
                            { key: 'bogus', value: 'x' },
                            { key: 'Location', value: 'GARAZ' },
                        ],
                    },
                });
                assert.equal(res.success, true);
                assert.equal(res.warnings.filter(w => w.includes('will be ignored')).length, 2);
                await waitFor(async () => Object.keys(await sendTo(harness, 'getEnabledDPs', {})).includes(objects.later));
                await harness.states.setStateAsync(objects.later, { val: 3, ack: true });
                const r = await waitFor(async () => (await sql(`SELECT * FROM numeric_states WHERE state_id = '${objects.later}' AND value = 3`))[0]);
                assert.equal(r.kind, 'early');
                assert.equal(r.bogus, undefined);
                assert.ok(r.location === undefined || r.location === null);
                assert.equal(r.site, 'home', 'catalog default applies to every state');
            });

            it('enableHistory works without restart and keeps the influxdb.0 settings untouched', async function () {
                this.timeout(30000);
                const res = await sendTo(harness, 'enableHistory', {
                    id: objects.later,
                    options: { changesOnly: false, customTags: [{ key: 'kind', value: 'late' }] },
                });
                assert.equal(res.success, true);
                await waitFor(async () => Object.keys(await sendTo(harness, 'getEnabledDPs', {})).includes(objects.later));
                await harness.states.setStateAsync(objects.later, { val: 7, ack: true });
                const r = await waitFor(async () => (await sql(`SELECT value, kind FROM numeric_states WHERE state_id = '${objects.later}' AND value = 7`))[0]);
                assert.deepEqual(r, { value: 7, kind: 'late' });

                await sendTo(harness, 'enableHistory', { id: objects.num, options: { debounceTime: 0 } });
                const obj = await harness.objects.getObjectAsync(objects.num);
                assert.deepEqual(obj.common.custom['influxdb.0'], classicCustom);
                assert.deepEqual(obj.common.custom['influx-v3.0'].customTags[0], { key: 'Location', value: 'KOTLOWNIA' });
            });

            it('disableHistory stops writing immediately', async function () {
                this.timeout(30000);
                await sendTo(harness, 'disableHistory', { id: objects.bool });
                await waitFor(async () => !Object.keys(await sendTo(harness, 'getEnabledDPs', {})).includes(objects.bool));
                const before = Number((await sql(`SELECT count(*) AS n FROM boolean_states WHERE state_id = '${objects.bool}'`))[0].n);
                await harness.states.setStateAsync(objects.bool, { val: false, ack: true });
                await sleep(1500);
                const after = Number((await sql(`SELECT count(*) AS n FROM boolean_states WHERE state_id = '${objects.bool}'`))[0].n);
                assert.equal(after, before);
            });

            it('changing the custom config of another instance does not affect this one', async function () {
                this.timeout(20000);
                const obj = await harness.objects.getObjectAsync(objects.num);
                obj.common.custom['influx-v3.1'] = { enabled: true, table: 'elsewhere' };
                await harness.objects.setObjectAsync(objects.num, obj);
                await sleep(1000);
                const dps = await sendTo(harness, 'getEnabledDPs', {});
                assert.equal(dps[objects.num].table, 'numeric_states');
            });

            it('SQL via sendTo: SELECT allowed, DELETE/INSERT refused', async function () {
                this.timeout(20000);
                const ok = await sendTo(harness, 'query', `SELECT count(*) AS n FROM numeric_states`);
                assert.equal(ok.error, null);
                assert.ok(Number(ok.result[0].n) >= 2);
                const bad = await sendTo(harness, 'query', 'DELETE FROM numeric_states');
                assert.match(bad.error, /read statements/);
                const bad2 = await sendTo(harness, 'query', { query: 'SELECT 1; INSERT INTO numeric_states VALUES (1)' });
                assert.ok(bad2.error);
            });

            it('diagnostics and statistics are available and contain no secrets', async function () {
                this.timeout(20000);
                const d = await sendTo(harness, 'diagnostics', {});
                assert.match(d.text, /Connected: yes/);
                const s = await sendTo(harness, 'getAdapterStats', {});
                assert.ok(s.written >= 4);
                if (TOKEN) {
                    assert.ok(!JSON.stringify(s).includes(TOKEN));
                }
            });

            it('two instances run at the same time with different databases, tables and tags', async function () {
                this.timeout(90000);
                const DB2 = 'iobharness2';
                // second instance object: copy of instance 0 (keeps the encrypted token) with another database
                const inst0 = await harness.objects.getObjectAsync('system.adapter.influx-v3.0');
                const inst1 = JSON.parse(JSON.stringify(inst0));
                inst1._id = 'system.adapter.influx-v3.1';
                inst1.common.enabled = true;
                inst1.native.database = DB2;
                inst1.native.tagDefinitions = CATALOG.map(d => (d.key === 'site' ? { ...d, defaultValue: 'second' } : d));
                await harness.objects.setObjectAsync(inst1._id, inst1);
                for (const o of ioPackage.instanceObjects) {
                    await harness.objects.setObjectAsync(`influx-v3.1.${o._id}`, { ...o, _id: `influx-v3.1.${o._id}` });
                }
                const both = `${BASE}.both`;
                const onlySecond = `${BASE}.onlySecond`;
                await harness.objects.setObjectAsync(
                    both,
                    stateObject('number', {
                        'influx-v3.0': { enabled: true, changesOnly: false, table: 'multi_a', customTags: [{ key: 'inst', value: 'zero' }] },
                        'influx-v3.1': { enabled: true, changesOnly: false, table: 'multi_b', customTags: [{ key: 'inst', value: 'one' }] },
                    }),
                );
                await harness.objects.setObjectAsync(onlySecond, stateObject('number', { 'influx-v3.1': { enabled: true, changesOnly: false } }));

                const proc = spawn('node', ['build/main.js', '--console', '--instance', '1'], { cwd: harness.testAdapterDir, stdio: ['ignore', 'pipe', 'pipe'] });
                let output = '';
                proc.stdout.on('data', d => (output += d));
                proc.stderr.on('data', d => (output += d));
                try {
                    await waitFor(async () => (await harness.states.getStateAsync('influx-v3.1.info.connection'))?.val === true, 30000);
                    await waitFor(async () => Object.keys(await sendTo(harness, 'getEnabledDPs', {})).includes(both));
                    const dps1 = await waitFor(async () => {
                        const r = await sendTo(harness, 'getEnabledDPs', {}, 'influx-v3.1');
                        return Object.keys(r).includes(both) && Object.keys(r).includes(onlySecond) ? r : null;
                    }, 30000);
                    assert.equal(dps1[both].table, 'multi_b');
                    const dps0 = await sendTo(harness, 'getEnabledDPs', {});
                    assert.equal(dps0[both].table, 'multi_a');
                    assert.ok(!Object.keys(dps0).includes(onlySecond));

                    await harness.states.setStateAsync(both, { val: 11, ack: true });
                    await harness.states.setStateAsync(onlySecond, { val: 22, ack: true });
                    const a = await waitFor(async () => (await sql(`SELECT value, inst, site FROM multi_a WHERE state_id = '${both}'`))[0]);
                    assert.deepEqual(a, { value: 11, inst: 'zero', site: 'home' });
                    const b = await waitFor(async () => (await sql(`SELECT value, inst, site FROM multi_b WHERE state_id = '${both}'`, DB2))[0]);
                    assert.deepEqual(b, { value: 11, inst: 'one', site: 'second' });
                    const c = await waitFor(async () => (await sql(`SELECT value FROM numeric_states WHERE state_id = '${onlySecond}'`, DB2))[0]);
                    assert.equal(c.value, 22);
                    const notInFirst = await sql(`SELECT count(*) AS n FROM numeric_states WHERE state_id = '${onlySecond}'`);
                    assert.equal(Number(notInFirst[0].n), 0);
                    await assert.rejects(() => sql(`SELECT * FROM multi_b`), /not found/, 'instance 0 never wrote table multi_b');
                } finally {
                    proc.kill('SIGTERM');
                    await new Promise(r => proc.once('exit', r));
                }
                assert.ok(!/uncaught|unhandled/i.test(output), output.slice(-2000));
            });

            it('stops cleanly and flushes the buffer', async function () {
                this.timeout(30000);
                await harness.states.setStateAsync(objects.num, { val: 99.5, ack: true });
                await harness.stopAdapter();
                const r = await waitFor(async () => (await sql(`SELECT value FROM numeric_states WHERE state_id = '${objects.num}' AND value = 99.5`))[0]);
                assert.equal(r.value, 99.5);
            });
        });

        (URL ? suite : () => undefined)('Migration with tag rules and import of historical data', getHarness => {
            let harness;
            const IMPORT_DB = 'iobimport';
            const M = `${BASE}.mig`;
            const ids = { temp: `${M}.temp`, aliased: `${M}.aliased`, sw: `${M}.switch` };
            const OLD_ALIAS = `${M}.old_alias`;
            // outside of `${M}.*`, so the import test below does not select them
            const O = `${BASE}.oldtags`;
            const oldIds = { kot: `${O}.kot`, garage: `${O}.garage` };
            // configured for this instance only, with a tag from before the tag catalog (ignored at runtime)
            const ownId = `${O}.own`;
            const RULES = [{ fromKey: 'room', fromValue: '', toKey: 'Location', toValue: '' }];
            const T = Date.UTC(2024, 0, 1);

            function seedOld() {
                const lines = [];
                for (let i = 0; i < 48; i++) {
                    lines.push(`${ids.temp},room=salon value=${20 + i / 10},ack=true,from="system.adapter.js.0",q=0 ${T + i * 3600000}`);
                    lines.push(`${OLD_ALIAS},room=Salon value=${i},ack=true,from="system.adapter.js.0",q=0 ${T + i * 3600000}`);
                }
                lines.push(`${ids.sw} value=true,ack=false,from="admin.0",q=0 ${T}`);
                lines.push(`${ids.sw} value=false,ack=true,from="admin.0",q=0 ${T + 60000}`);
                const u = new globalThis.URL(`${V2.INFLUX2_URL}/api/v2/write?org=${V2.INFLUX2_ORG}&bucket=${V2.INFLUX2_BUCKET}&precision=ms`);
                return new Promise((resolve, reject) => {
                    const req = http.request(
                        { hostname: u.hostname, port: u.port, path: `${u.pathname}${u.search}`, method: 'POST', headers: { Authorization: `Token ${V2.INFLUX2_TOKEN}` } },
                        res => {
                            res.resume();
                            res.on('end', () => (res.statusCode === 204 ? resolve() : reject(new Error(`seed: HTTP ${res.statusCode}`))));
                        },
                    );
                    req.on('error', reject);
                    req.end(lines.join('\n'));
                });
            }

            before(async function () {
                this.timeout(120000);
                harness = getHarness();
                const classic = extra => ({ 'influxdb.0': { enabled: true, changesOnly: true, debounceTime: 0, ...extra } });
                await harness.objects.setObjectAsync(
                    ids.temp,
                    stateObject('number', classic({ customTags: [{ name: 'room', value: 'kotlownia' }, { name: 'floor', value: '1' }] })),
                );
                await harness.objects.setObjectAsync(ids.aliased, stateObject('number', classic({ aliasId: OLD_ALIAS })));
                await harness.objects.setObjectAsync(ids.sw, stateObject('boolean', classic({})));
                await harness.objects.setObjectAsync(oldIds.kot, stateObject('number', classic({ customTags: [{ name: 'room', value: 'Kotłownia' }] })));
                await harness.objects.setObjectAsync(
                    ownId,
                    stateObject('number', { 'influx-v3.0': { enabled: true, customTags: [{ key: 'room', value: 'kotlownia' }] } }),
                );
                await harness.objects.setObjectAsync(
                    oldIds.garage,
                    stateObject('number', classic({ customTags: [{ name: 'room', value: 'Garaż' }, { name: 'floor', value: '2' }] })),
                );
                if (V2) {
                    await seedOld();
                }
                await harness.changeAdapterConfig('influx-v3', {
                    native: {
                        url: URL,
                        token: TOKEN,
                        database: IMPORT_DB,
                        flushIntervalMs: 200,
                        tagDefinitions: CATALOG,
                        migrationTagRules: RULES,
                        storeOnStart: false,
                        ...(V2
                            ? {
                                  dmVersion: '2.x',
                                  dmUrl: V2.INFLUX2_URL,
                                  dmOrg: V2.INFLUX2_ORG,
                                  dmBucket: V2.INFLUX2_BUCKET,
                                  dmToken: V2.INFLUX2_TOKEN,
                                  dmStates: `${M}.*`,
                                  dmInfluxdbInstance: 'influxdb.0',
                              }
                            : {}),
                    },
                });
                await harness.startAdapterAndWait();
                await waitFor(async () => (await harness.states.getStateAsync('influx-v3.0.info.connection'))?.val === true, 30000);
            });

            after(async function () {
                this.timeout(30000);
                await harness?.stopAdapter();
            });

            it('the live preview translates tags with the (unsaved) rules and lists what does not fit', async function () {
                this.timeout(20000);
                const withRules = await sendTo(harness, 'migrationPreviewHtml', { source: 'influxdb.0', overwrite: false, rules: RULES });
                assert.match(withRules.text, /room=kotlownia → Location=KOTLOWNIA \(rule 1\)/);
                assert.match(withRules.text, /floor=1 - \d+ state\(s\)/);
                assert.ok(!withRules.text.includes('<script'), 'escaped HTML only');
                const without = await sendTo(harness, 'migrationPreviewHtml', { source: 'influxdb.0', overwrite: false, rules: [] });
                assert.match(without.text, /room=kotlownia ✗ key &quot;room&quot; is not defined in the tag catalog/);
            });

            it('old keys and their values (several per key) are offered from the influxdb settings', async function () {
                this.timeout(20000);
                const keys = await sendTo(harness, 'getOldTagKeys', { source: 'influxdb.0' });
                const room = keys.find(k => k.value === 'room');
                assert.ok(room, JSON.stringify(keys));
                assert.match(room.label, /^room \(\d+ value\(s\), \d+ state\(s\)\)$/);
                assert.ok(keys.some(k => k.value === 'floor'));
                const values = (await sendTo(harness, 'getOldTagValues', { source: 'influxdb.0', key: 'ROOM' })).map(v => v.value);
                for (const v of ['kotlownia', 'Kotłownia', 'Garaż']) {
                    assert.ok(values.includes(v), `${v} in ${values}`);
                }
                assert.deepEqual(await sendTo(harness, 'getOldTagValues', { source: 'influxdb.0', key: 'nope' }), []);
            });

            // A rule table with 71 rows asks 142 times in parallel (old key + old values per row), as measured in Admin.
            it('many rule rows: 142 parallel dropdown requests are answered quickly, always with lists', async function () {
                this.timeout(60000);
                harness.setMaxListeners(500);
                const t0 = Date.now();
                const calls = [];
                for (let i = 0; i < 71; i++) {
                    calls.push(sendToMatched(harness, 'getOldTagKeys', { source: 'influxdb.0', includeOldPoints: !!V2 }));
                    calls.push(
                        sendToMatched(harness, 'getOldTagValues', {
                            source: 'influxdb.0',
                            key: 'room',
                            includeOldPoints: !!V2,
                            selected: ['a value that is gone'],
                        }),
                    );
                }
                const results = await Promise.all(calls);
                const ms = Date.now() - t0;
                assert.ok(results.every(Array.isArray), 'every answer is a list');
                assert.ok(ms < 20000, `answered in ${ms} ms`);
                const keys = results.filter((_, i) => i % 2 === 0);
                const values = results.filter((_, i) => i % 2 === 1);
                assert.ok(keys.every(k => k.some(x => x.value === 'room')), JSON.stringify(keys[0]));
                assert.ok(
                    values.every(v => v.some(x => x.value === 'a value that is gone' && /not used any more/.test(x.label))),
                    JSON.stringify(values[0]),
                );
                assert.ok(values.every(v => v.some(x => x.value === 'kotlownia')), 'values of the old key');
                // nonsense input still gives a list
                assert.deepEqual(await sendToMatched(harness, 'getOldTagValues', { key: 42 }), []);
            });

            it('"take over old tags" appends the needed rules with suggestions and keeps existing rows', async function () {
                this.timeout(20000);
                const res = await sendTo(harness, 'migrationSuggestRules', { source: 'influxdb.0', rules: RULES, catalog: CATALOG });
                assert.equal(res.result, 'rulesAdded');
                const rows = res.native.migrationTagRules;
                assert.deepEqual(rows[0], RULES[0], 'existing row unchanged');
                const kot = rows.find(r => r.fromKey === 'room' && r.toValue === 'KOTLOWNIA');
                assert.ok(kot && kot.fromValue.includes('Kotłownia') && kot.toKey === 'Location', JSON.stringify(rows));
                assert.ok(rows.some(r => r.fromKey === 'room' && r.fromValue.includes('Garaż') && r.toKey === 'Location' && r.toValue === ''));
                assert.ok(rows.some(r => r.fromKey === 'floor' && r.fromValue.length === 0 && r.toKey === ''));
                assert.equal(Number(res.args[0]), rows.length - 1);
            });

            it('"propose standard" matches keys/values automatically and extends catalog and rules', async function () {
                this.timeout(20000);
                const res = await sendTo(harness, 'migrationProposeStandard', {
                    source: 'influxdb.0',
                    rules: [],
                    catalog: CATALOG,
                    valueStyle: 'upper',
                    useEnums: true,
                });
                assert.equal(res.result, 'standardProposedShort', JSON.stringify(res));
                const cat = res.native.tagDefinitions;
                const rules = res.native.migrationTagRules;
                const location = cat.find(d => d.key === 'Location');
                assert.match(location.values, /^KOTLOWNIA, SALON, .*GARAZ/, 'missing value appended to the closed list');
                assert.ok(cat.some(d => d.key === 'floor' && /\b1\b/.test(d.values) && /\b2\b/.test(d.values)), 'new key for floor');
                assert.ok(rules.some(r => r.fromKey === 'room' && r.fromValue.length === 0 && r.toKey === 'Location'), 'room → Location (synonym)');
                assert.ok(rules.some(r => r.fromValue.includes('Kotłownia') && r.toValue === 'KOTLOWNIA'));
                assert.ok(rules.some(r => r.fromValue.includes('Garaż') && r.toValue === 'GARAZ'));
                assert.match(res.args[3], /^All \d+ used tag value\(s\) are covered/, 'short message');
                const details = (await harness.states.getStateAsync('influx-v3.0.info.migrationProposal')).val;
                assert.match(details, /room.* → Location/, 'details panel');
                // complete map: every old key has a key row, every used value a row of its own
                for (const k of await sendTo(harness, 'getOldTagKeys', { source: 'influxdb.0' })) {
                    assert.ok(rules.some(r => r.fromKey === k.value && r.fromValue.length === 0), `key row for ${k.value}`);
                    for (const v of await sendTo(harness, 'getOldTagValues', { source: 'influxdb.0', key: k.value })) {
                        assert.ok(rules.some(r => r.fromKey === k.value && r.fromValue.includes(v.value)), `row for ${k.value}=${v.value}`);
                    }
                }
                // with the proposal every old tag fits: the preview lists nothing that would be dropped
                const preview = await sendTo(harness, 'migrationPreviewHtml', { source: 'influxdb.0', overwrite: true, rules, catalog: cat });
                assert.ok(!/NOT written/.test(preview.text), preview.text);
                // a second proposal on top of the first one has nothing left to do
                const again = await sendTo(harness, 'migrationProposeStandard', { source: 'influxdb.0', rules, catalog: cat, valueStyle: 'upper' });
                assert.equal(again.result, 'standardNothing');
            });

            it('"propose standard" covers every used tag: settings, old points and this instance', async function () {
                this.timeout(60000);
                const res = await sendTo(harness, 'migrationProposeStandard', {
                    source: 'influxdb.0',
                    rules: [],
                    catalog: CATALOG,
                    valueStyle: 'upper',
                    includeOldPoints: !!V2,
                });
                assert.equal(res.result, 'standardProposedShort', JSON.stringify(res));
                assert.match(res.args[3], /^All \d+ used tag value\(s\) are covered/, res.args[3]);
                assert.equal(res.coverage.uncovered.length, 0);
                if (V2) {
                    const values = await sendTo(harness, 'getOldTagValues', { source: 'influxdb.0', key: 'room', includeOldPoints: true });
                    const salon = values.find(v => v.value === 'salon');
                    assert.ok(salon && /old points/.test(salon.label), JSON.stringify(values));
                }
            });

            it('apply standardises tags of states already configured for this instance', async function () {
                this.timeout(20000);
                const preview = await sendTo(harness, 'migrationPreviewHtml', { source: 'influxdb.0', rules: RULES, catalog: CATALOG, ids: [ownId] });
                assert.match(preview.text, /tags standardised by &quot;Apply&quot; \(1\)/);
                assert.match(preview.text, /room=kotlownia → Location=KOTLOWNIA/);
                const res = await sendTo(harness, 'migrationApply', { source: 'influxdb.0', rules: RULES, ids: [ownId] });
                assert.match(res.result, /tags of 1 already configured state\(s\) standardised/);
                const obj = await harness.objects.getObjectAsync(ownId);
                assert.deepEqual(obj.common.custom['influx-v3.0'].customTags, [{ key: 'Location', value: 'KOTLOWNIA' }]);
                assert.equal(obj.common.custom['influx-v3.0'].enabled, true, 'other settings untouched');
            });

            it('one rule with several old values is applied', async function () {
                this.timeout(20000);
                const rules = [...RULES, { fromKey: 'room', fromValue: ['Kotłownia', 'Garaż'], toKey: 'Location', toValue: 'KOTLOWNIA' }, { fromKey: 'floor', fromValue: [], toKey: '-', toValue: '' }];
                const html = await sendTo(harness, 'migrationPreviewHtml', { source: 'influxdb.0', rules, catalog: CATALOG, ids: Object.values(oldIds) });
                assert.match(html.text, /Custom tags in the settings of influxdb\.0/);
                assert.match(html.text, /room=Garaż → Location=KOTLOWNIA \(rule 2\)/);
                assert.match(html.text, /floor=2 → removed \(rule 3\)/);
                const res = await sendTo(harness, 'migrationApply', { source: 'influxdb.0', rules, ids: Object.values(oldIds) });
                assert.match(res.result, /2 state\(s\) configured/);
                for (const id of Object.values(oldIds)) {
                    const obj = await harness.objects.getObjectAsync(id);
                    assert.deepEqual(obj.common.custom['influx-v3.0'].customTags, [{ key: 'Location', value: 'KOTLOWNIA' }], id);
                }
            });

            it('apply writes the translated tags and leaves influxdb.0 untouched', async function () {
                this.timeout(20000);
                const res = await sendTo(harness, 'migrationApply', { source: 'influxdb.0', rules: RULES, ids: Object.values(ids) });
                assert.match(res.result, /3 state\(s\) configured for influx-v3\.0/);
                const temp = await harness.objects.getObjectAsync(ids.temp);
                assert.deepEqual(temp.common.custom['influx-v3.0'].customTags, [{ key: 'Location', value: 'KOTLOWNIA' }]);
                assert.deepEqual(temp.common.custom['influxdb.0'].customTags, [
                    { name: 'room', value: 'kotlownia' },
                    { name: 'floor', value: '1' },
                ]);
                const aliased = await harness.objects.getObjectAsync(ids.aliased);
                assert.equal(aliased.common.custom['influx-v3.0'].aliasId, OLD_ALIAS);
                await waitFor(async () => {
                    const dps = await sendTo(harness, 'getEnabledDPs', {});
                    return Object.values(ids).every(id => dps[id]);
                });
            });

            (V2 ? it : it.skip)('imports the history from InfluxDB 2.x: preview, start, progress, mapped tags', async function () {
                this.timeout(120000);
                const test = await sendTo(harness, 'dmTestSource', {});
                assert.match(test.result || test.error, /Connected to InfluxDB 2\.\d/);
                const preview = await sendTo(harness, 'dmPreview', {});
                assert.match(preview.result, /Preview ready/);
                const html = (await harness.states.getStateAsync('influx-v3.0.info.dataMigration.preview')).val;
                assert.match(html, /3 state\(s\) of this instance are selected/);
                assert.match(html, new RegExp(`old measurement &quot;${OLD_ALIAS.replace(/\./g, '\\.')}&quot;`));
                const start = await sendTo(harness, 'dmStart', {});
                assert.match(start.result, /Import of 3 state\(s\) started/);
                const status = await waitFor(async () => {
                    const st = (await harness.states.getStateAsync('influx-v3.0.info.dataMigration.status'))?.val || '';
                    return /^(finished|failed)/.test(st) ? st : null;
                }, 90000);
                assert.match(status, /^finished: 98 point\(s\) imported for 3 state\(s\)/);
                assert.equal((await harness.states.getStateAsync('influx-v3.0.info.dataMigration.progress')).val, 100);

                const q = query => sql(query, IMPORT_DB);
                const temp = await q(`SELECT COUNT(*) AS n, MIN(location) AS loc, MIN(value) AS vmin FROM numeric_states WHERE state_id = '${ids.temp}'`);
                assert.deepEqual([Number(temp[0].n), temp[0].loc, temp[0].vmin], [48, 'KOTLOWNIA', 20], 'the setting of the state wins over the old tag');
                const al = await q(`SELECT COUNT(*) AS n, MIN(location) AS loc FROM numeric_states WHERE state_id = '${OLD_ALIAS}'`);
                assert.deepEqual([Number(al[0].n), al[0].loc], [48, 'SALON'], 'stored under the alias like live values; old tag room=Salon → Location=SALON via rule');
                const sw = await q(`SELECT value, ack FROM boolean_states WHERE state_id = '${ids.sw}' ORDER BY time`);
                assert.deepEqual(sw.map(r => [r.value, r.ack]), [[true, false], [false, true]]);

                const again = await sendTo(harness, 'dmStart', {});
                assert.match(again.error, /already finished/);
                const reset = await sendTo(harness, 'dmReset', {});
                assert.match(reset.result, /Progress reset/);
                assert.ok(!harness.getLogs().some(l => l.message.includes(V2.INFLUX2_TOKEN)), 'token never logged');
            });
        });
    },
});
