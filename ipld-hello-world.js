import { CID } from 'multiformats/cid'
import * as Block from 'multiformats/block'
import { sha256 } from 'multiformats/hashes/sha2'
import * as raw from 'multiformats/codecs/raw'
import * as dagJSON from '@ipld/dag-json'
import * as dagCBOR from '@ipld/dag-cbor'
import { CarWriter } from '@ipld/car/writer'
import { CarReader } from '@ipld/car/reader'
import { parse } from 'ipld-schema'
import { create as createValidator } from 'ipld-schema-validator'
import _ from "lodash";
import fs from 'fs'
import { Readable } from 'stream'

/*
 * Schema DSL https://ipld.io/specs/schemas/schema-schema.ipldsch
 */

const SCHEMA = `

type Component struct {
  x Int
  y Int
  width Int
  height Int
  fill String
  id String
}

type Assembly [Component]
`

/*
 * Random data according to the schema spec
 */

const ORIGINAL_DATA = [
    {
        "x": 409,
        "y": 129,
        "width": 100,
        "height": 100,
        "fill": "#eeff41",
        "id": "rect1"
    },
    {
        "x": 278,
        "y": 340,
        "width": 112,
        "height": 100,
        "fill": "#ffab40",
        "id": "rect2"
    },
    {
        "x": 194,
        "y": 123,
        "width": 200,
        "height": 200,
        "fill": "#4285f4",
        "id": "rect3"
    },
    {
        "x": 410,
        "y": 246,
        "width": 254,
        "height": 251,
        "fill": "#0097a7",
        "id": "rect4"
    }
];

const PERSISTENCE_FILE = 'assembly.car';

async function run() {

    /*
     * Schema
     */
    const schema = parse(SCHEMA);

    console.dir(schema, { depth: Infinity });

    /*
     * Validate data w/ schema
     */
    validateAssemblySchema(ORIGINAL_DATA, schema);

    console.log('Initial assembly');
    console.dir(ORIGINAL_DATA, { depth: Infinity });

    /*
     * Create addressable data blocks
     */
    const { blocks, root } = await createAdressableBlocks();

    /*
     * Save blocks to file
     */
    await saveBlocks(blocks, root, PERSISTENCE_FILE);

    /*
     * Load assembly from file
     */
    const loaded = await loadAsembly(PERSISTENCE_FILE);

    console.log('Loaded assembly');
    console.dir(loaded, { depth: Infinity });

    /*
     * Check loaded equal to original
     */
    if (_.isEqual(loaded, ORIGINAL_DATA)) {
        console.log("Correct assembly loaded");
    } else {
        console.log("Loaded corrupted assembly");
    }
}

function validateAssemblySchema(data, schema) {

    const validateComponentFunction = createValidator(schema, 'Component');
    console.log('Validating data fragment as Component:', validateComponentFunction(data[0]));

    const validateAssemblyFunction = createValidator(schema, 'Assembly');
    console.log('Validating data as Assembly:', validateAssemblyFunction(data));

    if (!validateAssemblyFunction(data)) {
        throw "Invalid ORIGINAL_DATA";
    }
}


async function createAdressableBlocks() {

    const blocks = []

    /*
     * Custom persistence format, the assembly root only holds links to the actual assembly components
     */

    blocks.push(await encode(ORIGINAL_DATA[0]))
    blocks.push(await encode(ORIGINAL_DATA[1]))
    blocks.push(await encode(ORIGINAL_DATA[2]))
    blocks.push(await encode(ORIGINAL_DATA[3]))

    const cids = blocks.map(block => block.cid)

    /*
     * Custom root structure, links only
     */
    const assembly = await Block.encode({
        value: [
            { link: cids[0] },
            { link: cids[1] },
            { link: cids[2] },
            { link: cids[3] },
        ],
        hasher: sha256,
        codec: dagJSON
    });

    blocks.push(assembly);

    return { blocks, root: assembly.cid };
}


async function encode(obj) {

    return await Block.encode({ value: obj, hasher: sha256, codec: dagJSON });
}

async function decode(bytes) {

    return await Block.decode({ bytes, codec: dagJSON, hasher: sha256 });
}

async function saveBlocks(blocks, root, fileName) {
    const { writer, out } = await CarWriter.create(root);
    Readable.from(out).pipe(fs.createWriteStream(fileName));
    for (const block of blocks) {
        await writer.put(block);
    }
    await writer.close();
}


async function loadAsembly(fileName) {

    const inStream = fs.createReadStream(fileName);
    const reader = await CarReader.fromIterable(inStream);
    const roots = await reader.getRoots();
    const rootBlock = await reader.get(roots[0]);
    const bytes = rootBlock?.bytes;
    const rootDecoded = await decode(bytes);
    const rootElements = rootDecoded.value;

    // console.log('Root block decoded');
    // console.dir(rootElements, { depth: Infinity });

    const assembly = [];

    console.log('Root block elements');
    // @ts-ignore
    for (const element of rootElements) {
        console.log(element);
        const compBlock = await reader.get(element.link);
        const compBytes = compBlock?.bytes;
        const compDecoded = await decode(compBytes);
        assembly.push(compDecoded.value);
    }

    return assembly;
}

run().catch((err) => {
    console.error(err);
    process.exit(1);
});
