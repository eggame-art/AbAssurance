import assert from 'node:assert/strict';
import { after, afterEach, before, mock, test } from 'node:test';
import { createServer } from 'vite';
let vite, authController;
before(async()=>{
 vite=await createServer({server:{middlewareMode:true,hmr:false,ws:false},appType:'custom'});
 ({authController}=await vite.ssrLoadModule('/src/systems/auth/controllers/authController.ts'));
});
afterEach(()=>mock.restoreAll());
after(async()=>vite?.close());
test('login sends credentials and CSRF header, without supplying a role', async()=>{
 const user={username:'user',role:'user',source:'abassurance',source_id:'100'};
 const fetch=mock.method(globalThis,'fetch',async()=>Response.json(user));
 assert.deepEqual(await authController.login('user','test-password'),user);
 const [path,options]=fetch.mock.calls[0].arguments;
 assert.equal(path,'/api/auth/login');
 assert.equal(options.credentials,'include');
 assert.equal(options.headers['X-Requested-With'],'AbAssurance');
 assert.deepEqual(JSON.parse(options.body),{username:'user',password:'test-password'});
});
test('personal data calls contain no owner chosen by the browser',async()=>{
 const fetch=mock.method(globalThis,'fetch',async()=>Response.json({records:[],total:0}));
 await authController.getData('contracts',25);
 assert.equal(fetch.mock.calls[0].arguments[0],'/api/me/data/contracts?offset=25&limit=25');
});
test('wrong credentials and expired sessions reach the caller',async()=>{
 mock.method(globalThis,'fetch',async()=>Response.json({detail:'Connexion requise.'},{status:401}));
 await assert.rejects(authController.getSession(),{message:'Connexion requise.'});
});
