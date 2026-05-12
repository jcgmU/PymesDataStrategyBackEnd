import jwt from 'jsonwebtoken';

const token = jwt.sign(
  { userId: 'cmnrh2vgo0001qzdnva9ax5k6', email: 'test@example.com' },
  'dev-jwt-secret-change-in-production',
  { expiresIn: '1d' }
);
console.log(token);
