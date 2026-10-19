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

function sendTo(harness, command, message, instance = 'influx_v3.0') {
    return new Promise(resolve => harness.sendTo(instance, command, message, resolve));
}

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
                await harness.changeAdapterConfig('influx_v3', { native: { url: 'http://127.0.0.1:1', token: 'apiv3_unit', pingIntervalSec: 10 } });
                await harness.startAdapterAndWait();
                await sleep(2000);
                const conn = await harness.states.getStateAsync('influx_v3.0.info.connection');
                assert.equal(conn && conn.val, false);
                const obj = await harness.objects.getObjectAsync('influx_v3.0.info.queueRecords');
                assert.ok(obj, 'instance objects created');
                // the token is stored encrypted and never logged
                const inst = await harness.objects.getObjectAsync('system.adapter.influx_v3.0');
                assert.notEqual(inst.native.token, 'apiv3_unit');
                assert.ok(!harness.getLogs().some(l => l.message.includes('apiv3_unit')), 'token must not appear in logs');
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
                            'influx_v3.0': {
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
                await harness.objects.setObjectAsync(objects.bool, stateObject('boolean', { 'influx_v3.0': { enabled: true, changesOnly: false } }));
                await harness.objects.setObjectAsync(objects.str, stateObject('string', { 'influx_v3.0': { enabled: true } }));
                await harness.objects.setObjectAsync(objects.other, stateObject('number', { 'influx_v3.1': { enabled: true } }));
                await harness.objects.setObjectAsync(objects.later, stateObject('number', {}));
                await harness.changeAdapterConfig('influx_v3', {
                    native: {
                        url: URL,
                        token: TOKEN,
                        database: DB,
                        flushIntervalMs: 200,
                        globalTags: [{ key: 'site', value: 'home' }],
                        autoTagChannel: true,
                        storeOnStart: false,
                    },
                });
                await harness.startAdapterAndWait();
                await waitFor(async () => (await harness.states.getStateAsync('influx_v3.0.info.connection'))?.val === true);
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
                assert.equal(Number(other[0].n), 0, 'state of influx_v3.1 must not be written by influx_v3.0');
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
                const config = { ...obj.common.custom['influx_v3.0'], table: 'heating' };
                const res = await sendTo(harness, 'previewMapping', { id: objects.num, config });
                assert.match(res.text, /Table: heating/);
                assert.match(res.text, /location=KOTLOWNIA/);
                assert.match(res.text, /sub_type=HC1/);
                const dps = await sendTo(harness, 'getEnabledDPs', {});
                assert.equal(dps[objects.num].table, 'numeric_states', 'preview does not change the configuration');
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
                const r = await waitFor(async () => (await sql(`SELECT value, kind FROM numeric_states WHERE state_id = '${objects.later}'`))[0]);
                assert.deepEqual(r, { value: 7, kind: 'late' });

                await sendTo(harness, 'enableHistory', { id: objects.num, options: { debounceTime: 0 } });
                const obj = await harness.objects.getObjectAsync(objects.num);
                assert.deepEqual(obj.common.custom['influxdb.0'], classicCustom);
                assert.deepEqual(obj.common.custom['influx_v3.0'].customTags[0], { key: 'Location', value: 'KOTLOWNIA' });
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
                obj.common.custom['influx_v3.1'] = { enabled: true, table: 'elsewhere' };
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
                const inst0 = await harness.objects.getObjectAsync('system.adapter.influx_v3.0');
                const inst1 = JSON.parse(JSON.stringify(inst0));
                inst1._id = 'system.adapter.influx_v3.1';
                inst1.common.enabled = true;
                inst1.native.database = DB2;
                inst1.native.globalTags = [{ key: 'site', value: 'second' }];
                await harness.objects.setObjectAsync(inst1._id, inst1);
                for (const o of ioPackage.instanceObjects) {
                    await harness.objects.setObjectAsync(`influx_v3.1.${o._id}`, { ...o, _id: `influx_v3.1.${o._id}` });
                }
                const both = `${BASE}.both`;
                const onlySecond = `${BASE}.onlySecond`;
                await harness.objects.setObjectAsync(
                    both,
                    stateObject('number', {
                        'influx_v3.0': { enabled: true, changesOnly: false, table: 'multi_a', customTags: [{ key: 'inst', value: 'zero' }] },
                        'influx_v3.1': { enabled: true, changesOnly: false, table: 'multi_b', customTags: [{ key: 'inst', value: 'one' }] },
                    }),
                );
                await harness.objects.setObjectAsync(onlySecond, stateObject('number', { 'influx_v3.1': { enabled: true, changesOnly: false } }));

                const proc = spawn('node', ['build/main.js', '--console', '--instance', '1'], { cwd: harness.testAdapterDir, stdio: ['ignore', 'pipe', 'pipe'] });
                let output = '';
                proc.stdout.on('data', d => (output += d));
                proc.stderr.on('data', d => (output += d));
                try {
                    await waitFor(async () => (await harness.states.getStateAsync('influx_v3.1.info.connection'))?.val === true, 30000);
                    await waitFor(async () => Object.keys(await sendTo(harness, 'getEnabledDPs', {})).includes(both));
                    const dps1 = await waitFor(async () => {
                        const r = await sendTo(harness, 'getEnabledDPs', {}, 'influx_v3.1');
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
    },
});
